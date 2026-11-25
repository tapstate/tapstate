package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.runtime.engine.ChainAxes;
import io.tapstate.runtime.engine.FrontierOrders;
import io.tapstate.runtime.engine.ReplayFloor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class AMissingObjectIsSettledWhenItsLookupSnapshotEndsTest {

    @Test
    void allTwoHundredOrdersSurviveAndTheEightLateCustomersCompleteTheirExistingDocuments() throws Exception {
        Fixture f = new Fixture(Map.of("customers", 1L));
        for (int key = 1; key <= 200; key++) {
            if (key % 25 != 0) {
                f.customer(key, SourceOrder.snapshotRow(1));
            }
        }
        for (int key = 1; key <= 200; key++) {
            f.order(key, key, SourceOrder.snapshotRow(1));
            if (key % 10 != 0) {
                f.line(key);
            }
        }
        assertThat(f.latest()).hasSize(192);
        f.boundary("orders", 1);
        assertThat(f.latest()).hasSize(192);
        f.boundary("customers", 1);
        assertThat(f.latest()).hasSize(200);
        for (int key = 1; key <= 200; key++) {
            Map<String, Object> expected = row("order_id", key, "cust_ref", key,
                    "lines", key % 10 == 0 ? List.of() : List.of(lineRow(key)));
            if (key % 25 != 0) {
                expected.put("customer", customerRow(key));
            }
            assertThat(f.latest().get(key)).isEqualTo(expected);
        }

        int before = f.documents.size();
        for (int key = 25; key <= 200; key += 25) {
            f.customer(key, new SourceOrder(1, key));
        }
        assertThat(f.documents).hasSize(before + 8);
        assertThat(f.latest()).hasSize(200);
        for (int key = 25; key <= 200; key += 25) {
            assertThat(f.latest().get(key)).containsEntry("customer", customerRow(key));
        }
    }

    @Test
    void everyProducerMustCrossTheExpectedGenerationBeforeAbsenceIsKnown() throws Exception {
        Fixture f = new Fixture(Map.of("customer-a", 2L, "customer-b", 2L));
        f.order(25, 25, SourceOrder.snapshotRow(2));
        f.boundary("customer-a", 1);
        f.boundary("customer-a", 2);
        f.boundary("customer-b", 1);
        assertThat(f.documents).isEmpty();
        f.boundary("customer-b", 2);
        assertThat(f.latest()).containsKey(25);
    }

    @Test
    void aLateFirstFilingAfterRestartUsesThePersistedMissingIdentity() throws Exception {
        Fixture f = new Fixture(Map.of("customers", 1L));
        f.order(25, 25, SourceOrder.snapshotRow(1));
        f.boundary("customers", 1);
        assertThat(f.latest().get(25)).doesNotContainKey("customer");
        f.restart(25);
        int before = f.documents.size();
        f.customer(25, new SourceOrder(2, 1));
        assertThat(f.documents).hasSize(before + 1);
        assertThat(f.latest().get(25)).containsEntry("customer", customerRow(25));
        f.wake(new NestTouch(List.of(25), 3L, Map.of(), false, true));
        assertThat(f.documents).hasSize(before + 1);
    }

    @Test
    void theUnemittedSnapshotRootStaysBelowTheFrontierAndCanBeReplayedAfterRestart() throws Exception {
        Fixture f = new Fixture(Map.of("customers", 1L));
        f.order(25, 25, SourceOrder.snapshotRow(1));
        f.boundary("orders", 1);
        long reserved = FrontierOrders.pack("orders", SourceOrder.snapshotRow(1));
        assertThat(f.emitted.stream().filter(Watermark.class::isInstance).map(Watermark.class::cast)
                .filter(w -> w.key() == f.axes.axisOf("orders")).map(Watermark::timestamp).toList())
                .isNotEmpty().allSatisfy(bound -> assertThat(bound).isLessThan(reserved));
        f.restart(25);
        f.order(25, 25, SourceOrder.snapshotRow(1));
        f.boundary("orders", 1);
        f.boundary("customers", 1);
        assertThat(f.latest()).containsKey(25);
        assertThat(f.emitted.stream().filter(Watermark.class::isInstance).map(Watermark.class::cast)
                .filter(w -> w.key() == f.axes.axisOf("orders")).map(Watermark::timestamp).toList())
                .contains(reserved);
    }

    @Test
    void oldSerializedRootsAreReDeterminedRatherThanAssumedComplete() throws Exception {
        Fixture f = new Fixture(Map.of("customers", 1L));
        try (var input = getClass().getResourceAsStream("/nest/root-uid2-before-missing-object-field.base64")) {
            assertThat(input).isNotNull();
            byte[] original = Base64.getMimeDecoder().decode(input.readAllBytes());
            assertThat(new String(original, StandardCharsets.ISO_8859_1)).doesNotContain("missingObjectReferences");
            try (var object = new ObjectInputStream(new ByteArrayInputStream(original))) {
                RootAssembly legacy = (RootAssembly) object.readObject();
                assertThat(legacy.mayNeedReferenceUpdate()).isTrue();
                f.roots.save(List.of(1), legacy);
            }
        }
        Envelope registration = Envelope.insert(1L, "orders", row("order_id", 1, "cust_ref", "missing"), null)
                .withOrder(new SourceOrder(1, 1));
        feed(f.lookup, LookupProcessor.REGISTRATIONS, registration);
        f.customerKey("missing", new SourceOrder(2, 1));
        assertThat(f.latest().get(1)).containsEntry("customer", row("customer_id", "missing", "name", "customer-missing"));
    }

    @Test
    void newOrdersAfterAnEmptySnapshotAreEmittedAndCanBeCompletedLater() throws Exception {
        Fixture f = new Fixture(Map.of("customers", 1L));
        f.boundary("customers", 1);
        f.order(25, 25, new SourceOrder(1, 1));
        assertThat(f.latest().get(25)).doesNotContainKey("customer");
        f.customer(25, new SourceOrder(1, 2));
        assertThat(f.latest().get(25)).containsEntry("customer", customerRow(25));
    }

    @Test
    void aCachedCompleteRootCanRepointToAMissingCustomerAndBeCompletedLater() throws Exception {
        Fixture f = new Fixture(Map.of("customers", 1L));
        f.customer(1, SourceOrder.snapshotRow(1));
        f.order(1, 1, SourceOrder.snapshotRow(1));
        f.boundary("customers", 1);
        f.order(1, 2, new SourceOrder(1, 1));
        assertThat(f.latest().get(1)).containsEntry("cust_ref", 2).doesNotContainKey("customer");
        int before = f.documents.size();
        f.customer(2, new SourceOrder(1, 2));
        assertThat(f.documents).hasSize(before + 1);
        assertThat(f.latest().get(1)).containsEntry("customer", customerRow(2));
    }

    @Test
    void aDelayedRegistrationAfterRetargetingCannotLowerTheSnapshotHold() throws Exception {
        Fixture f = new Fixture(Map.of("customers", 1L));
        f.customer(1, SourceOrder.snapshotRow(1));
        SourceOrder first = new SourceOrder(1, 1);
        SourceOrder second = new SourceOrder(1, 2);
        Envelope original = Envelope.insert(1L, "orders", row("order_id", 1, "cust_ref", 1), null)
                .withOrder(first).withPositions(Map.of("orders", new ChainPosition(first, null)));
        feed(f.assembler, 0, original);
        f.collect();
        assertThat(f.documents).hasSize(1);
        assertThat(f.latest().get(1)).containsEntry("customer", customerRow(1));
        f.bound("orders", first);

        Envelope updated = Envelope.update(2L, "orders", original.after(), row("order_id", 1, "cust_ref", 2), null)
                .withOrder(second).withPositions(Map.of("orders", new ChainPosition(second, null)));
        feed(f.assembler, 0, updated);
        f.collect();
        assertThat(f.documents).hasSize(1);

        feed(f.lookup, LookupProcessor.REGISTRATIONS, original);
        f.route();
        feed(f.lookup, LookupProcessor.REGISTRATIONS, updated);
        f.route();
        f.bound("orders", second);
        assertThat(f.documents).hasSize(1);
        assertThat(f.emitted.stream().filter(Watermark.class::isInstance).map(Watermark.class::cast)
                .filter(w -> w.key() == f.axes.axisOf("orders")).map(Watermark::timestamp).toList())
                .containsExactly(FrontierOrders.pack("orders", first));

        f.customer(2, SourceOrder.snapshotRow(1));
        assertThat(f.documents).hasSize(2);
        assertThat(f.latest().get(1)).containsEntry("cust_ref", 2).containsEntry("customer", customerRow(2));
        assertThat(f.documents.getLast().positions()).containsEntry("orders", new ChainPosition(second, null));
        f.bound("orders", second);
        f.bound("orders", new SourceOrder(1, 3));
        assertThat(f.emitted.stream().filter(Watermark.class::isInstance).map(Watermark.class::cast)
                .filter(w -> w.key() == f.axes.axisOf("orders")).map(Watermark::timestamp).toList())
                .contains(FrontierOrders.pack("orders", second), FrontierOrders.pack("orders", new SourceOrder(1, 3)));
    }

    @Test
    void withoutSnapshotEligibilityACdcBoundDoesNotInventAbsence() throws Exception {
        Fixture f = new Fixture(Map.of());
        f.order(25, 25, new SourceOrder(1, 1));
        f.boundary("customers", 1);
        assertThat(f.documents).isEmpty();
        f.customer(25, new SourceOrder(1, 2));
        assertThat(f.latest()).containsKey(25);
    }

    @ParameterizedTest
    @EnumSource(value = EmbedAs.class, names = {"ARRAY", "FLAT"})
    void snapshotSettlementDoesNotChangeOtherReferenceShapes(EmbedAs shape) throws Exception {
        Fixture f = new Fixture(Map.of("customers", 1L), shape, 128, NestSendPolicy.everyChange());
        f.order(25, 25, SourceOrder.snapshotRow(1));
        f.boundary("customers", 1);
        assertThat(f.documents).isEmpty();
    }

    @Test
    void snapshotSettlementResumesAcrossBackpressureAndFlushesFoldedChanges() throws Exception {
        Fixture f = new Fixture(Map.of("customers", 1L), EmbedAs.OBJECT, 1, NestSendPolicy.within(1_000));
        f.customer(1, SourceOrder.snapshotRow(1));
        f.order(1, 1, SourceOrder.snapshotRow(1));
        f.order(1, 25, new SourceOrder(1, 1));
        for (int key = 2; key <= 300; key++) {
            f.order(key, key, SourceOrder.snapshotRow(1));
        }
        f.boundary("customers", 1);
        f.now.set(1_001);
        for (int attempt = 0; attempt < 1_000; attempt++) {
            boolean done = f.assembler.tryProcess();
            f.collect();
            if (done) {
                break;
            }
        }
        assertThat(f.latest()).hasSize(300);
        assertThat(f.latest().get(1)).containsEntry("cust_ref", 25).doesNotContainKey("customer");
    }

    private static Map<String, Object> lineRow(int order) {
        return row("order_id", order, "item_id", 1, "sku", "item-" + order);
    }

    private static Map<String, Object> customerRow(Object key) {
        return row("customer_id", key, "name", "customer-" + key);
    }

    private static void feed(Processor processor, int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(List.of(items));
        processor.process(ordinal, inbox);
        assertThat(inbox).isEmpty();
    }

    private static final class Fixture {
        private final NestTopology topology;
        private final NestLookup customers;
        private final HeapNestStore<RootAssembly> roots = new HeapNestStore<>();
        private final HeapNestStore<Map<String, Object>> rows = new HeapNestStore<>();
        private final HeapNestStore<Set<Object>> references = new HeapNestStore<>();
        private final Map<String, Long> epochs;
        private final ChainAxes axes;
        private final int capacity;
        private final NestSendPolicy policy;
        private final AtomicLong now = new AtomicLong();
        private final List<Object> emitted = new ArrayList<>();
        private final List<Envelope> documents = new ArrayList<>();
        private TestOutbox output;
        private TestOutbox lookupOutput;
        private AssemblerProcessor assembler;
        private LookupProcessor lookup;

        private Fixture(Map<String, Long> epochs) throws Exception {
            this(epochs, EmbedAs.OBJECT, 128, NestSendPolicy.everyChange());
        }

        private Fixture(Map<String, Long> epochs, EmbedAs shape, int capacity, NestSendPolicy policy) throws Exception {
            this.epochs = epochs;
            this.capacity = capacity;
            this.policy = policy;
            topology = NestTopology.compile("p", "doc", nest("order", List.of("order_id"),
                    embed("item", "order_id", "order_id", EmbedAs.ARRAY, "lines", List.of("item_id")),
                    embed("customer", "customer_id", "cust_ref", shape,
                            shape == EmbedAs.FLAT ? null : "customer", null)), tables());
            customers = topology.lookups().getFirst();
            Set<String> chains = new LinkedHashSet<>(List.of("orders", "lines", "customers"));
            chains.addAll(epochs.keySet());
            axes = ChainAxes.assign(chains);
            start();
        }

        private void start() throws Exception {
            List<String> customerChains = epochs.isEmpty() ? List.of("customers") : List.copyOf(epochs.keySet());
            output = new TestOutbox(capacity);
            assembler = new AssemblerProcessor(topology.assembler(), topology.slots(), roots, "doc", axes,
                    Map.of(0, List.of("orders"), 1, List.of("lines"), customers.touchOrdinal(), customerChains),
                    ReplayFloor.NONE, NestSettings.defaults(), now::get, policy, new HeapNestStore<>(),
                    (from, released) -> { throw new AssertionError("unexpected dead letter"); },
                    Map.of(customers.mapName(), rows), false, epochs.isEmpty() ? Map.of()
                            : Map.of(customers.mapName(), new LookupSnapshot(customers.touchOrdinal(), epochs)));
            assembler.init(output, new TestProcessorContext());
            lookupOutput = new TestOutbox(4_096);
            lookup = new LookupProcessor(customers, rows, references, 10_000, null);
            lookup.init(lookupOutput, new TestProcessorContext());
        }

        private void order(int key, Object customer, SourceOrder order) {
            Envelope event = Envelope.insert(key, "orders", row("order_id", key, "cust_ref", customer), null)
                    .withOrder(order).withPositions(Map.of("orders", new ChainPosition(order, null)));
            feed(assembler, 0, event);
            collect();
            feed(lookup, LookupProcessor.REGISTRATIONS, event);
            route();
        }

        private void line(int key) {
            feed(assembler, 1, Envelope.insert(key, "items", lineRow(key), null).withOrder(SourceOrder.snapshotRow(1)));
            collect();
        }

        private void customer(int key, SourceOrder order) {
            customerKey(key, order);
        }

        private void customerKey(Object key, SourceOrder order) {
            feed(lookup, LookupProcessor.ROWS, Envelope.insert(1L, "customers", customerRow(key), null)
                    .withOrder(order).withPositions(Map.of("customers", new ChainPosition(order, null))));
            route();
        }

        private void route() {
            List<Object> pending = new ArrayList<>();
            lookupOutput.drainQueueAndReset(0, pending, false);
            for (Object item : pending) {
                feed(assembler, customers.touchOrdinal(), item);
                collect();
            }
        }

        private void wake(NestTouch touch) {
            feed(assembler, customers.touchOrdinal(), touch);
            collect();
        }

        private void boundary(String chain, long epoch) {
            bound(chain, SourceOrder.snapshotRow(epoch));
        }

        private void bound(String chain, SourceOrder order) {
            int ordinal = chain.equals("orders") ? 0 : customers.touchOrdinal();
            Watermark bound = new Watermark(FrontierOrders.pack(chain, order), axes.axisOf(chain));
            for (int attempt = 0; attempt < 10_000; attempt++) {
                boolean accepted = assembler.tryProcessWatermark(ordinal, bound);
                collect();
                if (accepted) {
                    return;
                }
            }
            throw new AssertionError("snapshot boundary did not settle");
        }

        private void collect() {
            List<Object> pending = new ArrayList<>();
            output.drainQueueAndReset(0, pending, false);
            emitted.addAll(pending);
            pending.stream().filter(Envelope.class::isInstance).map(Envelope.class::cast)
                    .filter(event -> event.after() != null).forEach(documents::add);
        }

        private Map<Object, Map<String, Object>> latest() {
            Map<Object, Map<String, Object>> latest = new LinkedHashMap<>();
            documents.forEach(event -> latest.put(event.after().get("order_id"), event.after()));
            return latest;
        }

        private void restart(int key) throws Exception {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (var output = new ObjectOutputStream(bytes)) {
                output.writeObject(roots.load(List.of(key)));
            }
            try (var input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                roots.save(List.of(key), (RootAssembly) input.readObject());
            }
            start();
        }
    }
}
