package io.tapstate.runtime.engine;

import com.hazelcast.config.Config;
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
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.SourceColumn;
import io.tapstate.core.sql.SourceTable;
import io.tapstate.core.sql.SqlFrontEnd;
import io.tapstate.runtime.engine.join.DimensionRowDisplacedAlert;
import io.tapstate.runtime.engine.join.JoinDag;
import io.tapstate.runtime.engine.join.JoinFrontier;
import io.tapstate.runtime.engine.join.JoinStoresBinding;
import io.tapstate.runtime.engine.join.JoinUpdate;
import io.tapstate.core.sql.JoinKey;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** A real Jet Join whose target writes and per-source durable acknowledgements must agree. */
class JoinAdvancesTheDurableFrontierTest {

    private static final String ORDERS = "orders";
    private static final String CUSTOMERS = "customers";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(ORDERS, CUSTOMERS));
    private static final SourceOrder ORDER_AT = new SourceOrder(1, 7);
    private static final SourceOrder CUSTOMER_AT = new SourceOrder(1, 9);
    private static final List<String> ACKED = Collections.synchronizedList(new ArrayList<>());
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());
    private static final Map<String, Map<String, ChainPosition>> PHYSICAL_ACKED =
            Collections.synchronizedMap(new LinkedHashMap<>());
    private static final AtomicBoolean UPDATE_RELEASED = new AtomicBoolean();
    private static final AtomicBoolean BLOCK_SECOND_GRACE = new AtomicBoolean();
    private static final AtomicInteger GRACE_WRITES = new AtomicInteger();
    private static final AtomicInteger GRACE_SETTLED_AT_ACK = new AtomicInteger(-1);
    private static volatile CompletableFuture<Void> TAIL_GATE;

    private HazelcastInstance member;
    private Job job;

    @BeforeEach
    void startMember() {
        ACKED.clear();
        WRITTEN.clear();
        PHYSICAL_ACKED.clear();
        UPDATE_RELEASED.set(false);
        BLOCK_SECOND_GRACE.set(false);
        GRACE_WRITES.set(0);
        GRACE_SETTLED_AT_ACK.set(-1);
        TAIL_GATE = new CompletableFuture<>();
        Config config = new Config();
        config.setClusterName("join-ack-" + UUID.randomUUID());
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (job != null) {
            job.cancel();
        }
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void aJoinedTargetRowAcknowledgesBothSourceChains() {
        run(List.of(new Row(row("o_id", 10L, "o_cust_id", 1L), ORDER_AT)),
                List.of(new Row(row("c_id", 1L, "c_name", "Ada"), CUSTOMER_AT)));

        await(() -> written().stream().anyMatch(event ->
                event.after() != null && "Ada".equals(event.after().get("customer_name"))));
        assertThat(written()).anySatisfy(event -> assertThat(event.after())
                .containsEntry("order_id", 10L).containsEntry("customer_name", "Ada"));

        await(() -> acked().containsAll(List.of(ack(ORDERS, ORDER_AT), ack(CUSTOMERS, CUSTOMER_AT))));
        assertThat(acked()).contains(ack(ORDERS, ORDER_AT), ack(CUSTOMERS, CUSTOMER_AT));
    }

    @Test
    void aDimensionWithNoFactRowStillAcknowledgesItsSourcePosition() {
        run(List.of(), List.of(new Row(row("c_id", 99L, "c_name", "Unused"), CUSTOMER_AT)));

        await(() -> acked().contains(ack(CUSTOMERS, CUSTOMER_AT)));
        assertThat(written()).isEmpty();
    }

    @Test
    void aQuietTableEdgeDoesNotPinTheOtherTableOnTheirSharedPhysicalChain() {
        run(List.of(), List.of(new Row(row("c_id", 99L, "c_name", "Unused"), CUSTOMER_AT)));

        await(() -> acked().contains(ack(CUSTOMERS, CUSTOMER_AT)));
        synchronized (PHYSICAL_ACKED) {
            assertThat(PHYSICAL_ACKED).containsOnlyKeys("shared-mining-chain");
            assertThat(PHYSICAL_ACKED.get("shared-mining-chain"))
                    .containsKey(CUSTOMERS)
                    .doesNotContainKey(ORDERS);
            assertThat(PHYSICAL_ACKED.get("shared-mining-chain").get(CUSTOMERS).token())
                    .isEqualTo("token-customers-9");
        }
        assertThat(written()).isEmpty();
    }

    @Test
    void aDimensionFanOutWaitsForItsLastTargetWriteAcrossProjectionPartitions() {
        BLOCK_SECOND_GRACE.set(true);
        List<Row> facts = new ArrayList<>();
        for (long id = 10; id < 26; id++) {
            facts.add(new Row(row("o_id", id, "o_cust_id", 1L),
                    new SourceOrder(1, id)));
        }
        SourceOrder changedAt = new SourceOrder(1, 10);
        run(facts, List.of(
                new Row(row("c_id", 1L, "c_name", "Ada"), CUSTOMER_AT),
                new Row(row("c_id", 1L, "c_name", "Grace"),
                        row("c_id", 1L, "c_name", "Ada"), changedAt, true)));

        await(() -> written().stream().filter(event -> event.after() != null
                && "Ada".equals(event.after().get("customer_name"))).count() == facts.size());
        UPDATE_RELEASED.set(true);
        await(() -> GRACE_WRITES.get() >= 2);
        assertThat(acked()).doesNotContain(ack(CUSTOMERS, changedAt));

        TAIL_GATE.complete(null);
        await(() -> acked().contains(ack(CUSTOMERS, changedAt)));
        assertThat(GRACE_SETTLED_AT_ACK.get())
                .as("all fan-out target writes settled before the dimension position was acknowledged")
                .isEqualTo(facts.size());
    }

    private void run(List<Row> orders, List<Row> customers) {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT o.o_id AS order_id, c.c_name AS customer_name "
                        + "FROM orders o LEFT JOIN customers c ON o.o_cust_id = c.c_id",
                List.of(new SourceTable(ORDERS, List.of(
                        new SourceColumn("o_id", TapstateType.INT64, false),
                        new SourceColumn("o_cust_id", TapstateType.INT64, true))),
                        new SourceTable(CUSTOMERS, List.of(
                                new SourceColumn("c_id", TapstateType.INT64, false),
                                new SourceColumn("c_name", TapstateType.STRING, false)))));
        DAG dag = new DAG();
        Map<String, Vertex> sources = Map.of(
                "o", dag.newVertex(ORDERS, source(ORDERS, orders)),
                "c", dag.newVertex(CUSTOMERS, source(CUSTOMERS, customers)));
        Map<Vertex, Integer> outbound = new HashMap<>();
        Vertex joined = JoinDag.attach(dag, plan, "pipeline", "joined", List.of("o_id"),
                Map.of("c", List.of("c_id")), alias -> List.of(sources.get(alias)),
                vertex -> outbound.merge(vertex, 1, Integer::sum) - 1,
                JoinStoresBinding.onTheCluster(), DimensionRowDisplacedAlert.NONE,
                new JoinFrontier(AXES, alias -> List.of(List.of(
                        alias.equals("o") ? ORDERS : CUSTOMERS))));
        if (orders.size() > 1) {
            @SuppressWarnings("unchecked")
            com.hazelcast.jet.core.Partitioner<Object> partitioner =
                    (com.hazelcast.jet.core.Partitioner<Object>)
                            dag.getInboundEdges("joined:project").getFirst().getPartitioner();
            partitioner.init(key -> Math.floorMod(key.hashCode(), 4));
            Set<Integer> destinations = new HashSet<>();
            for (Row order : orders) {
                String key = JoinKey.of(List.of(order.fields().get("o_id"))).name();
                destinations.add(partitioner.getPartition(new JoinUpdate(key,
                        Envelope.insert(1L, "joined", Map.of("order_id", order.fields().get("o_id")), null)), 4));
            }
            assertThat(destinations).as("fact keys reach several projection partition buckets")
                    .hasSizeGreaterThan(1);
        }
        Vertex sink = dag.newVertex("sink", ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new SinkProcessor(
                        new RecordingWriter(),
                        JoinAdvancesTheDurableFrontierTest::recordAck,
                        new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN),
                        1, 1))));
        dag.edge(Edge.from(joined, outbound.merge(joined, 1, Integer::sum) - 1)
                .to(sink, 0).distributed());
        job = JetJobs.submit(member, dag, "join-ack");
    }

    private record Row(Map<String, Object> fields, Map<String, Object> before,
            SourceOrder order, boolean gated) implements java.io.Serializable {
        private Row(Map<String, Object> fields, SourceOrder order) {
            this(fields, null, order, false);
        }
    }

    private static ProcessorMetaSupplier source(String stream, List<Row> rows) {
        List<Row> plan = List.copyOf(rows);
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new RowsThenBounds(stream, plan)));
    }

    private static final class RowsThenBounds extends AbstractProcessor {
        private final String stream;
        private final List<Row> rows;
        private int next;
        private long bound = FrontierOrders.pack(ORDERS, new SourceOrder(1, 900));

        RowsThenBounds(String stream, List<Row> rows) {
            this.stream = stream;
            this.rows = rows;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
            while (next < rows.size()) {
                Row row = rows.get(next);
                if (row.gated() && !UPDATE_RELEASED.get()) {
                    return false;
                }
                ChainPosition position = new ChainPosition(row.order(), "token-" + stream + "-" + row.order().seq());
                Envelope event = row.before() == null
                        ? Envelope.insert(next + 1L, stream, row.fields(), null)
                        : Envelope.update(next + 1L, stream, row.before(), row.fields(), null);
                if (!tryEmit(event.withPosition(position))) {
                    return false;
                }
                next++;
            }
            if (!tryEmit(new Watermark(bound, AXES.axisOf(stream)))) {
                return false;
            }
            bound++;
            return false;
        }
    }

    private static final class RecordingWriter implements SinkWriter {
        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            Envelope event = records.getFirst();
            if (BLOCK_SECOND_GRACE.get() && event.after() != null
                    && "Grace".equals(event.after().get("customer_name"))
                    && GRACE_WRITES.incrementAndGet() == 2) {
                return TAIL_GATE.thenApply(ignored -> {
                    WRITTEN.addAll(records);
                    return new WriteResult(records.size());
                });
            }
            WRITTEN.addAll(records);
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            result.put((String) fields[i], fields[i + 1]);
        }
        return result;
    }

    private static String ack(String chain, SourceOrder order) {
        return chain + ":" + order.epoch() + ":" + order.seq();
    }

    /** Both table axes map to one durable mining-chain identity after the sink frontier settles them. */
    private static void recordAck(String table, ChainPosition position) {
        if (BLOCK_SECOND_GRACE.get() && table.equals(CUSTOMERS) && position.order().seq() == 10) {
            synchronized (WRITTEN) {
                GRACE_SETTLED_AT_ACK.set((int) WRITTEN.stream().filter(event ->
                        event.after() != null && "Grace".equals(event.after().get("customer_name"))).count());
            }
        }
        ACKED.add(ack(table, position.order()));
        synchronized (PHYSICAL_ACKED) {
            PHYSICAL_ACKED.computeIfAbsent("shared-mining-chain", ignored -> new LinkedHashMap<>())
                    .put(table, position);
        }
    }

    private static List<String> acked() {
        synchronized (ACKED) {
            return List.copyOf(ACKED);
        }
    }

    private static List<Envelope> written() {
        synchronized (WRITTEN) {
            return List.copyOf(WRITTEN);
        }
    }

    private void await(java.util.function.BooleanSupplier reached) {
        JobWatch.until(job, Duration.ofSeconds(12), reached,
                () -> "written=" + written() + ", acked=" + acked());
    }
}
