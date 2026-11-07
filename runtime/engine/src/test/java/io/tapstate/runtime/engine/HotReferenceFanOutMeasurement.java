package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.DirectNestStore;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestDag;
import io.tapstate.runtime.engine.nest.NestFrontier;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Measures what one edit to a hot referenced row costs: one customer pointed at by every order, renamed
 * once every document exists. The edit fans out as one touch per order, routed by root key, so on two
 * members about half of them cross the network, and each assembler drain reads the customer once.
 *
 * <p>Not a test of correctness and not run by default. Enable with {@code -Dnest.fanout=true}; size with
 * {@code -Dfanout.orders}, {@code -Dfanout.members} (1 or 2) and the store with
 * {@code -Dtapstate.nest.store=map|direct}; {@code -Dtapstate.nest.direct.in-memory-latency-micros} puts a
 * simulated round trip in front of every store call on the direct path. Prints one {@code NEST-FANOUT}
 * line.
 */
@EnabledIfSystemProperty(named = "nest.fanout", matches = "true")
class HotReferenceFanOutMeasurement {

    private static final String ORDERS_CHAIN = "orders";
    private static final String CUSTOMERS = "customers";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(ORDERS_CHAIN, CUSTOMERS));
    private static final String CUSTOMER = "C1";
    private static final String BEFORE = "Ada-before";
    private static final String AFTER = "Grace-after";
    private static final long FAR_ABOVE = FrontierOrders.pack(CUSTOMERS, new SourceOrder(1, 1L << 40));

    /** Latest customer name per order. Static: the job runs on the members, in this process. */
    private static final Map<Object, String> LATEST = new ConcurrentHashMap<>();
    private static final AtomicLong DOCUMENTS = new AtomicLong();
    private static volatile boolean editReleased;

    @Test
    void hotReferenceEdit() {
        int orders = Integer.getInteger("fanout.orders", 2_000);
        int members = Integer.getInteger("fanout.members", 2);
        String mode = System.getProperty("tapstate.nest.store", "map");
        String cluster = "nest-fanout-" + System.nanoTime();
        List<HazelcastInstance> cluster0 = new ArrayList<>();
        for (int i = 0; i < members; i++) cluster0.add(Hazelcast.newHazelcastInstance(clustered(cluster)));
        Job job = null;
        try {
            assertThat(cluster0.getFirst().getCluster().getMembers()).hasSize(members);
            LATEST.clear();
            DOCUMENTS.set(0);
            editReleased = false;
            long started = System.nanoTime();
            job = run(cluster0.getFirst(), orders, members);
            await(() -> LATEST.size() == orders && LATEST.values().stream().allMatch(BEFORE::equals), 600);
            long built = System.nanoTime();
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            DirectNestStore.counters().reset();
            long documentsBefore = DOCUMENTS.get();
            long released = System.nanoTime();
            editReleased = true;
            await(() -> LATEST.values().stream().allMatch(AFTER::equals), 600);
            long converged = System.nanoTime();
            var c = DirectNestStore.counters();
            System.out.printf("NEST-FANOUT {\"mode\":\"%s\",\"members\":%d,\"orders\":%d,\"latencyMicros\":%d,"
                            + "\"buildMs\":%d,\"editMs\":%d,\"docsAfterEdit\":%d,\"readThroughCalls\":%d,"
                            + "\"readThroughKeys\":%d,\"commits\":%d,\"keysCommitted\":%d,\"storeCalls\":%d}%n",
                    mode, members, orders, Long.getLong("tapstate.nest.direct.in-memory-latency-micros", 0L),
                    TimeUnit.NANOSECONDS.toMillis(built - started), TimeUnit.NANOSECONDS.toMillis(converged - released),
                    DOCUMENTS.get() - documentsBefore, c.readThroughCalls(), c.readThroughKeys(), c.commits(),
                    c.keysCommitted(), c.storeCalls());
        } finally {
            if (job != null) job.cancel();
            for (int i = cluster0.size() - 1; i >= 0; i--) cluster0.get(i).shutdown();
            DirectNestStore.forgetUnbacked();
        }
    }

    private static Config clustered(String cluster) {
        Config config = new Config();
        config.setClusterName(cluster);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(15711).setPortAutoIncrement(true).setPortCount(2);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).setMembers(List.of("127.0.0.1:15711", "127.0.0.1:15712"));
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setTypeClass(Envelope.class).setImplementation(new EnvelopeSerializer()));
        return config;
    }

    private static Job run(HazelcastInstance member, int count, int members) {
        Embed customer = new Embed("c", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("o", List.of("order_id"), null, null, List.of(customer)));
        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("o", new NestTable(ORDERS_CHAIN, List.of("order_id")));
        tables.put("c", new NestTable(CUSTOMERS, List.of("customer_id")));

        DAG dag = new DAG();
        Map<String, Vertex> byAlias = new LinkedHashMap<>();
        byAlias.put("o", dag.newVertex(ORDERS_CHAIN, source(ORDERS_CHAIN, count, false)));
        byAlias.put("c", dag.newVertex(CUSTOMERS, source(CUSTOMERS, 0, true)));
        Map<String, String> chainOfAlias = Map.of("o", ORDERS_CHAIN, "c", CUSTOMERS);
        Map<Vertex, Integer> outbound = new HashMap<>();
        Vertex assembled = NestDag.attach(dag,
                NestTopology.compile("p", "doc", body, tables::get),
                "doc", "o", "doc",
                alias -> List.of(byAlias.get(alias)),
                new NestBinding(tables::get, NestBinding.onMap(), (from, released) -> { }),
                vertex -> outbound.merge(vertex, 1, Integer::sum) - 1,
                new NestFrontier(AXES, alias -> List.of(List.of(chainOfAlias.get(alias)))),
                new NodeWidth("doc", 2, members, null));
        Vertex collector = dag.newVertex("collect", ProcessorSupplier.of((SupplierEx<Processor>) Collecting::new));
        dag.edge(Edge.from(assembled, outbound.merge(assembled, 1, Integer::sum) - 1).to(collector, 0).distributed());
        return JetJobs.submit(member, dag, "nest-fanout");
    }

    private static final class Collecting extends AbstractProcessor {
        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            if (item instanceof Envelope envelope && envelope.after() != null
                    && envelope.after().get("customer") instanceof Map<?, ?> c) {
                DOCUMENTS.incrementAndGet();
                LATEST.put(envelope.after().get("order_id"), String.valueOf(c.get("name")));
            }
            return true;
        }
    }

    private static ProcessorMetaSupplier source(String stream, int orders, boolean customers) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(
                (SupplierEx<Processor>) () -> new Feed(stream, orders, customers)));
    }

    /** Orders, or the customer then (once released) its edit; then bounds for as long as the job runs. */
    private static final class Feed extends AbstractProcessor {
        private final String stream;
        private final int orders;
        private final boolean customers;
        private int next;
        private boolean edited;
        private boolean boundRefused;
        private long bound = FAR_ABOVE;

        Feed(String stream, int orders, boolean customers) {
            this.stream = stream;
            this.orders = orders;
            this.customers = customers;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
            // A refused offer must be offered again before anything else.
            if (boundRefused) {
                if (!tryEmit(new Watermark(bound, AXES.axisOf(stream)))) return false;
                boundRefused = false;
                bound++;
            }
            if (customers) {
                if (next == 0) {
                    if (!tryEmit(Envelope.insert(1L, stream, row("customer_id", CUSTOMER, "name", BEFORE), null)
                            .withOrder(new SourceOrder(1, 1)))) return false;
                    next = 1;
                }
                if (editReleased && !edited) {
                    if (!tryEmit(Envelope.insert(2L, stream, row("customer_id", CUSTOMER, "name", AFTER), null)
                            .withOrder(new SourceOrder(1, 2)))) return false;
                    edited = true;
                }
            } else {
                while (next < orders) {
                    int i = next + 1;
                    if (!tryEmit(Envelope.insert(i, stream, row("order_id", i, "cust_ref", CUSTOMER), null)
                            .withOrder(new SourceOrder(1, i)))) return false;
                    next++;
                }
            }
            if (!tryEmit(new Watermark(bound, AXES.axisOf(stream)))) {
                boundRefused = true;
                return false;
            }
            bound++;
            return false;
        }
    }

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2) row.put((String) fields[i], fields[i + 1]);
        return row;
    }

    private static void await(BooleanSupplier reached, int seconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            if (reached.getAsBoolean()) return;
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        throw new AssertionError("never converged: " + LATEST.size() + " documents, "
                + LATEST.values().stream().filter(AFTER::equals).count() + " edited");
    }
}
