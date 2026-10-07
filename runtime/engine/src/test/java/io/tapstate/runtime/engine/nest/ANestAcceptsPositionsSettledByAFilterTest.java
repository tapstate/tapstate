package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tracking;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Partitioner;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestSupport;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.SettledPositions;
import io.tapstate.runtime.engine.TransformProcessor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** A filter's settlement travels through nest routing and lookup bookkeeping without becoming a row. */
class ANestAcceptsPositionsSettledByAFilterTest {

    private static final TransformBody.Nest POINTING = nest("order", List.of("order_id"),
            embed("customer", "customer_id", "cust_ref", EmbedAs.OBJECT, "customer", null));
    private static final NestLookup CUSTOMERS =
            NestTopology.compile("p", "doc", POINTING, tables()).lookups().get(0);

    private final HeapNestStore<Map<String, Object>> rows = new HeapNestStore<>();
    private final HeapNestStore<Set<Object>> references = new HeapNestStore<>();

    @Test
    void everySourceEdgeRoutesTheOutputOfAFilterIncludingDepartureAndLookupEdges() throws Exception {
        TransformBody.Nest tree = nest(new NestRoot("customer", List.of("customer_id"), null, true,
                List.of(tracking(embed("order", "customer_id", "customer_id", EmbedAs.ARRAY, "orders",
                                List.of("order_id"), tracking(embed("item", "order_id", "order_id",
                                        EmbedAs.ARRAY, "items", List.of("item_id"))))),
                        embed("profile", "customer_id", "profile_ref", EmbedAs.OBJECT, "profile", null))));
        DAG dag = new DAG();
        Map<String, Vertex> filters = new LinkedHashMap<>();
        for (String alias : List.of("customer", "order", "item", "profile")) {
            filters.put(alias, dag.newVertex(alias, TransformProcessor.metaSupplier(alias,
                    () -> event -> Boolean.FALSE.equals(event.after().get("keep"))
                            ? List.of() : List.of(event))));
        }
        Map<Vertex, Integer> outgoing = new LinkedHashMap<>();
        NestTopology topology = NestTopology.compile("p", "doc", tree, tables());
        NestDag.attach(dag, topology, "doc", "customer", "doc", alias -> List.of(filters.get(alias)),
                new NestBinding(tables(), HeapNestStores.onHeap(), (from, released) -> { }),
                vertex -> outgoing.merge(vertex, 1, Integer::sum) - 1, null);
        assertThat(topology.lookups()).hasSize(1);
        assertThat(topology.vertices()).hasSize(2);

        int routed = 0;
        Integer settlementPartition = null;
        for (Map.Entry<String, Vertex> filter : filters.entrySet()) {
            Envelope kept = Envelope.insert(1L, filter.getKey(), row("customer_id", 1, "profile_ref", 1,
                    "order_id", 10, "item_id", 100), null).withOrder(at(1));
            Envelope dropped = Envelope.insert(2L, filter.getKey(), row("keep", false), null)
                    .withOrder(at(2));
            List<Object> filtered = filter(kept, dropped);
            assertThat(filtered).containsExactly(kept, new SettledPositions(dropped.positions()));
            for (Edge edge : dag.getOutboundEdges(filter.getValue().getName())) {
                @SuppressWarnings("unchecked")
                Partitioner<Object> partitioner = (Partitioner<Object>) edge.getPartitioner();
                partitioner.init(key -> Math.floorMod(key.hashCode(), 271));
                partitioner.getPartition(filtered.get(0), 271);
                int partition = partitioner.getPartition(filtered.get(1), 271);
                if (settlementPartition == null) {
                    settlementPartition = partition;
                }
                assertThat(partition).isEqualTo(settlementPartition);
                routed++;
            }
        }
        assertThat(routed).isEqualTo(9);
    }

    @Test
    void droppedLookupRowsSettleBehindTheWakesOfKeptRows() throws Exception {
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor lookup = lookup(outbox);
        Envelope order = order(1, 100);
        feed(lookup, LookupProcessor.REGISTRATIONS, List.of(order));
        assertThat(drain(outbox)).isEmpty();

        Envelope kept = customer(100, 1);
        Envelope dropped = Envelope.insert(2L, "customer", row("customer_id", 200, "keep", false), null)
                .withOrder(at(2));
        feed(lookup, LookupProcessor.ROWS, filter(kept, dropped));

        assertThat(drain(outbox)).containsExactly(
                new NestTouch(List.of(1), kept.ts(), kept.positions(), false, true),
                new SettledPositions(dropped.positions()));
        assertThat(rows.count()).isEqualTo(1);
        assertThat(rows.load(List.of(100))).isEqualTo(kept.after());
        assertThat(rows.load(List.of(200))).isNull();
    }

    @Test
    void lookupSettlementsStayBehindEarlierWakesWhenTheOutboxIsFull() throws Exception {
        Envelope kept = customer(100, 1);
        Envelope dropped = Envelope.insert(2L, "customer", row("keep", false), null).withOrder(at(2));
        TestSupport.verifyProcessor(() -> {
            HeapNestStore<Set<Object>> pointing = new HeapNestStore<>();
            Object bucket = NestLookup.bucketKey(List.of(100), NestLookup.bucketOf(List.of(1)));
            pointing.save(bucket, Set.of(List.of(1)));
            return new LookupProcessor(CUSTOMERS, new HeapNestStore<>(), pointing, 100L, null);
        }).input(filter(kept, dropped))
                .expectOutput(List.of(new NestTouch(List.of(1), kept.ts(), kept.positions(), false, true),
                        new SettledPositions(dropped.positions())));
    }

    @Test
    void aQueuedLookupSettlementResumesWithoutAnotherRowArriving() throws Exception {
        TestOutbox outbox = new TestOutbox(1);
        LookupProcessor lookup = lookup(outbox);
        feed(lookup, LookupProcessor.REGISTRATIONS, List.of(order(1, 100)));
        Envelope kept = customer(100, 1);
        Envelope dropped = Envelope.insert(2L, "customer", row("keep", false), null).withOrder(at(2));

        feed(lookup, LookupProcessor.ROWS, filter(kept, dropped));
        assertThat(drain(outbox)).containsExactly(
                new NestTouch(List.of(1), kept.ts(), kept.positions(), false, true));

        assertThat(lookup.tryProcess()).isTrue();
        assertThat(drain(outbox)).containsExactly(new SettledPositions(dropped.positions()));
        assertThat(lookup.complete()).isTrue();
        assertThat(drain(outbox)).isEmpty();
    }

    @Test
    void registrationDrainsIgnoreDroppedRowsWhileRegisteringTheKeptOnes() throws Exception {
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor lookup = lookup(outbox);
        Envelope kept = order(1, 100);
        Envelope dropped = Envelope.insert(2L, "order", row("order_id", 2, "cust_ref", 100, "keep", false), null)
                .withOrder(at(2));
        List<Object> filtered = filter(kept, dropped);

        feed(lookup, LookupProcessor.REGISTRATIONS, filtered);
        feed(lookup, LookupProcessor.DEPARTED_REGISTRATIONS, filtered);

        assertThat(drain(outbox)).isEmpty();
        assertThat(rows.count()).isZero();
        Object bucket = NestLookup.bucketKey(List.of(100), NestLookup.bucketOf(List.of(1)));
        assertThat(references.load(bucket)).containsExactly(List.of(1));
        assertThat(references.count()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void aDrainOfOnlyDroppedRowsChangesNoStateAndOnlySpeaksForTheRowsEdge(int ordinal) throws Exception {
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor lookup = lookup(outbox);
        Envelope dropped = Envelope.insert(1L, "customer", row("keep", false), null).withOrder(at(1));
        List<Object> filtered = filter(dropped);

        feed(lookup, ordinal, filtered);

        assertThat(drain(outbox)).isEqualTo(ordinal == LookupProcessor.ROWS ? filtered : List.of());
        assertThat(rows.count()).isZero();
        assertThat(references.count()).isZero();
    }

    private LookupProcessor lookup(TestOutbox outbox) throws Exception {
        LookupProcessor processor = new LookupProcessor(CUSTOMERS, rows, references, 100L, null);
        processor.init(outbox, new TestProcessorContext());
        return processor;
    }

    private static List<Object> filter(Envelope... events) throws Exception {
        TransformProcessor processor = new TransformProcessor(event ->
                Boolean.FALSE.equals(event.after().get("keep")) ? List.of() : List.of(event));
        TestOutbox outbox = new TestOutbox(1024);
        processor.init(outbox, new TestProcessorContext());
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(List.of(events));
        processor.process(0, inbox);
        assertThat(inbox).isEmpty();
        return drain(outbox);
    }

    private static void feed(LookupProcessor lookup, int ordinal, List<Object> items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(items);
        lookup.process(ordinal, inbox);
        assertThat(inbox).isEmpty();
    }

    private static List<Object> drain(TestOutbox outbox) {
        List<Object> sent = new ArrayList<>();
        outbox.drainQueueAndReset(0, sent, false);
        return sent;
    }

    private static Envelope order(int id, int customerId) {
        return Envelope.insert(id, "order", row("order_id", id, "cust_ref", customerId), null)
                .withOrder(at(id));
    }

    private static Envelope customer(int id, int sequence) {
        return Envelope.insert(sequence, "customer", row("customer_id", id, "name", "Ada"), null)
                .withOrder(at(sequence));
    }
}
