package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Partitioner;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Every entry a vertex touches while handling an event is on the partition that event was routed to.
 *
 * <p>This is what lets a nest vertex keep its state in a distributed map at all. Each edge into a vertex
 * is partitioned by the key that vertex files its state under, so the event and the entry it needs meet
 * on the same member and the same partition, and no vertex ever reaches across the cluster for state.
 * Break the alignment and nothing fails: every read still returns the right value, fetched over the
 * network from wherever it happens to live. The job merely becomes slower in a way no counter names -
 * and it degenerates into the shape this design was chosen over.
 *
 * <p>It cannot be observed by running anything here. A single member owns every partition, so a
 * misaligned key is fetched from itself and even the slowdown does not happen; the day it is wrong is
 * the day a second member joins, long after the code that broke it was written. So the two halves are
 * compared directly instead: the partition the edge's own partitioner routes the item to, against the
 * partition the map computes for each key the vertex then actually touched. The keys are taken from a
 * store that records what it was asked for rather than being derived a second time here, because a
 * second derivation would only agree with itself.
 *
 * <p>Both kinds of vertex and both kinds of edge are walked - rows arriving from a source and elements
 * cascaded up from a resolver below - and the cascaded ones are the very items a resolver emitted a
 * moment earlier rather than items written out by hand.
 *
 * <p>Each edge is probed with several distinct keys rather than one. Two different keys land on the same
 * partition roughly once in every few hundred, so a single probe lets a genuinely misaligned key pass by
 * coincidence - which is not hypothetical: it happened while this case was being checked against a
 * deliberately broken routing, and the case stayed green. Several keys make that coincidence have to
 * repeat, and it does not.
 */
class NestStateSitsOnThePartitionItsEventsArriveOnTest {

    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))),
            embed("order", "customer_id", "customer_id", EmbedAs.ARRAY, "orders", List.of("order_id"),
                    embed("item", "order_id", "order_id", EmbedAs.ARRAY, "items", List.of("item_id"))));

    private static final List<String> ALIASES = List.of("customer", "policy", "claim", "order", "item");

    private HazelcastInstance member;
    private NestTopology topology;
    private DAG dag;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);

        topology = NestTopology.compile("p", "doc", TREE, tables());
        dag = new DAG();
        Map<String, Vertex> sources = new java.util.LinkedHashMap<>();
        for (String alias : ALIASES) {
            sources.put(alias, dag.newVertex(alias, Processors.noopP()));
        }
        NestDag.attach(dag, topology, "doc", "customer", "doc",
                alias -> List.of(sources.get(alias)),
                new NestBinding(tables(), NestBinding.onHeap(), element -> { }),
                vertex -> 0, null);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void everyVertexTouchesOnlyEntriesOnThePartitionItsEventWasRoutedTo() {
        NestVertex policies = topology.vertexAt(List.of("policies"));
        NestVertex orders = topology.vertexAt(List.of("orders"));
        NestVertex assembler = topology.assembler();

        List<Object> upFromPolicies = aligned(policies, 0, events(3, seq -> event(seq,
                "policy_id", "P" + seq, "customer_id", "C" + seq, "policy_no", "PN-" + seq)));
        aligned(policies, 1, events(3, seq -> event(seq, "claim_id", "CL" + seq, "policy_id", "P" + seq)));

        List<Object> upFromOrders = aligned(orders, 0, events(3, seq -> event(seq,
                "order_id", "O" + seq, "customer_id", "C" + seq)));
        aligned(orders, 1, events(3, seq -> event(seq, "item_id", "I" + seq, "order_id", "O" + seq)));

        aligned(assembler, 0, events(3, seq -> event(seq, "customer_id", "C" + seq)));
        assertThat(upFromPolicies)
                .describedAs("a resolver passed elements upward, so the cascading edge has real items")
                .hasSize(3);
        assertThat(upFromOrders)
                .describedAs("a resolver passed elements upward, so the cascading edge has real items")
                .hasSize(3);
        aligned(assembler, ordinalOf(assembler, List.of("policies")), upFromPolicies);
        aligned(assembler, ordinalOf(assembler, List.of("orders")), upFromOrders);
    }

    /**
     * Feeds each of {@code items} to {@code vertex} on {@code ordinal} one at a time, asserts every entry
     * the vertex touched is on the partition the edge routed that item to, and hands back everything the
     * vertex passed on. Each item gets a store of its own so that what one of them touched cannot be read
     * as another's.
     */
    private List<Object> aligned(NestVertex vertex, int ordinal, List<Object> items) {
        List<Object> emitted = new ArrayList<>();
        for (Object item : items) {
            RecordingStore<Object> store = new RecordingStore<>();
            emitted.addAll(feed(vertex, ordinal, item, store));

            assertThat(store.touched)
                    .describedAs("%s touched state while handling what arrived on ordinal %d, so there "
                            + "is something to compare", vertex.name(), ordinal)
                    .isNotEmpty();
            int routedTo = partitionOfEdge(vertex, ordinal, item);
            for (Object key : store.touched) {
                assertThat(partitionOf(key))
                        .describedAs("%s files %s on the partition its event arrived on", vertex.name(), key)
                        .isEqualTo(routedTo);
            }
        }
        return emitted;
    }

    /** {@code count} items differing in the key they carry, so one coincidental collision cannot hide. */
    private static List<Object> events(int count, LongFunction<Envelope> shape) {
        List<Object> items = new ArrayList<>();
        for (long seq = 1; seq <= count; seq++) {
            items.add(shape.apply(seq));
        }
        return items;
    }

    @SuppressWarnings("unchecked")
    private List<Object> feed(NestVertex vertex, int ordinal, Object item, RecordingStore<Object> store) {
        Processor processor = vertex.isAssembler()
                ? new AssemblerProcessor(vertex, topology.slots(),
                        (NestStore<RootAssembly>) (NestStore<?>) store, "doc")
                : new ResolverProcessor(vertex, (NestStore<ResolverState>) (NestStore<?>) store,
                        element -> { });
        TestOutbox outbox = new TestOutbox(128);
        try {
            processor.init(outbox, new TestProcessorContext());
        } catch (Exception cause) {
            throw new IllegalStateException("could not start " + vertex.name(), cause);
        }
        TestInbox inbox = new TestInbox();
        inbox.queue().add(item);
        processor.process(ordinal, inbox);
        List<Object> emitted = new ArrayList<>();
        outbox.drainQueueAndReset(0, emitted, false);
        return emitted;
    }

    /** The partition the edge into {@code vertex} on {@code ordinal} routes {@code item} to. */
    @SuppressWarnings("unchecked")
    private int partitionOfEdge(NestVertex vertex, int ordinal, Object item) {
        Edge edge = dag.getInboundEdges(vertex.name()).stream()
                .filter(candidate -> candidate.getDestOrdinal() == ordinal)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "no edge into " + vertex.name() + " on ordinal " + ordinal));
        Partitioner<Object> partitioner = (Partitioner<Object>) edge.getPartitioner();
        assertThat(partitioner)
                .describedAs("the edge into %s on ordinal %d is partitioned at all - an unpartitioned one "
                        + "would send every item everywhere", vertex.name(), ordinal)
                .isNotNull();
        partitioner.init(this::partitionOf);
        return partitioner.getPartition(item, member.getPartitionService().getPartitions().size());
    }

    /** The partition a distributed map computes for {@code key} - where the entry under it lives. */
    private int partitionOf(Object key) {
        return member.getPartitionService().getPartition(key).getPartitionId();
    }

    private static int ordinalOf(NestVertex vertex, List<String> pathId) {
        return vertex.inboundFor(pathId).ordinal();
    }

    private static Envelope event(long seq, Object... pairs) {
        return Envelope.insert(seq, "src", row(pairs), null).withOrder(new SourceOrder(1L, seq));
    }

    /** A store that answers like any other and remembers which keys it was asked about. */
    private static final class RecordingStore<S> implements NestStore<S> {

        private static final long serialVersionUID = 1L;

        private final transient Set<Object> touched = new LinkedHashSet<>();
        private final HeapNestStore<S> delegate = new HeapNestStore<>();

        @Override
        public S load(Object key) {
            touched.add(key);
            return delegate.load(key);
        }

        @Override
        public void save(Object key, S state) {
            touched.add(key);
            delegate.save(key, state);
        }

        @Override
        public void remove(Object key) {
            touched.add(key);
            delegate.remove(key);
        }

        @Override
        public long count() {
            return delegate.count();
        }
    }
}
