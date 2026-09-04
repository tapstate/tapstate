package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
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
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.HeapNestStores;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestDag;
import io.tapstate.runtime.engine.nest.NestFrontier;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * That the stream of rows documents merely <em>point at</em> is a stream the frontier can get past.
 *
 * <p><b>Nothing else in this tree can move it, and that is why it is worth a case of its own.</b> Those
 * rows are filed by a vertex that assembles nothing, and until that vertex had somewhere to send, no edge
 * anywhere downstream was compiled to carry their chain - so no level ever worked out a bound on it and no
 * document ever reported a position on it. Both halves are needed and each is silent on its own: a bound
 * that never reaches a document, and a document that goes out saying nothing about the row it drew in.
 *
 * <p>What that costs is invisible from anywhere the run can be watched. The job stays RUNNING, every
 * document is right, no count is off - the only symptom is a source whose retention window eventually runs
 * out because nothing ever told it that stream had been consumed. So this asserts the one thing that says
 * otherwise: a position on that chain written down by the sink.
 */
class TheFrontierReachesTheStreamOfRowsDocumentsPointAtTest {

    private static final String ORDERS = "orders";
    private static final String CUSTOMERS = "customers";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(ORDERS, CUSTOMERS));

    private static final SourceOrder ORDER_AT = new SourceOrder(1, 3);
    private static final SourceOrder CUSTOMER_AT = new SourceOrder(1, 4);

    /** Far above anything that arrives, so nothing here is short of a bound to advance under. */
    private static final long FAR_ABOVE = FrontierOrders.pack(CUSTOMERS, new SourceOrder(1, 900));

    /** Every position the sink wrote down, as {@code chain:epoch:seq}. Static: the job runs on the member. */
    private static final List<String> ACKED = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;
    private Job job;

    @BeforeEach
    void startMember() {
        ACKED.clear();
        Config config = new Config();
        config.setClusterName("nest-reference-frontier-test-" + System.nanoTime());
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
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
    void aPositionOnThePointedAtStreamIsWrittenDownLikeAnyOther() {
        run();

        // The root's own chain being written down first proves the sink is advancing at all, so anything
        // missing below is a decision rather than a job that never got going.
        await(() -> ACKED.contains(acked(ORDERS, ORDER_AT)));
        await(() -> ACKED.contains(acked(CUSTOMERS, CUSTOMER_AT)));

        assertThat(ACKED)
                .describedAs("the customer row reached a sink inside the document that drew it in, and the "
                        + "frontier is told so. Its stream has no other path to a document at all, so "
                        + "without that the chain stands still for the life of the job while every "
                        + "document is right and nothing anywhere reads as wrong")
                .contains(acked(ORDERS, ORDER_AT), acked(CUSTOMERS, CUSTOMER_AT));
    }

    private static void await(BooleanSupplier until) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!until.getAsBoolean() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
        }
    }

    private static String acked(String chain, SourceOrder at) {
        return chain + ":" + at.epoch() + ":" + at.seq();
    }

    private static void record(String chain, ChainPosition position) {
        ACKED.add(acked(chain, position.order()));
    }

    // ---- the job under test ------------------------------------------------------------

    /** One order pointing at one customer, ending in a sink that advances what it has confirmed. */
    private void run() {
        Embed customer = new Embed("cu", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT, "customer",
                null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("o", List.of("order_id"), null, null, List.of(customer)));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("o", new NestTable(ORDERS, List.of("order_id")));
        tables.put("cu", new NestTable(CUSTOMERS, List.of("customer_id")));

        DAG dag = new DAG();
        Map<String, Vertex> byAlias = new LinkedHashMap<>();
        byAlias.put("o", dag.newVertex(ORDERS, source(ORDERS,
                List.of(new Row(row("order_id", "O1", "cust_ref", "C1"), ORDER_AT)))));
        // Held back so the order is recorded as pointing at it before it lands. Which way round they arrive
        // decides what carries the customer's position downstream, and this case is about the path that
        // exists: the row arrives, wakes the document naming it, and travels inside the version that goes
        // out. Arriving first it is read straight into a document instead, and nothing carries it at all -
        // see the case below, which is why that is not left to a race.
        byAlias.put("cu", dag.newVertex(CUSTOMERS, source(CUSTOMERS,
                List.of(new Row(row("customer_id", "C1", "name", "Ada"), CUSTOMER_AT)), 700)));

        Map<String, String> chainOfAlias = Map.of("o", ORDERS, "cu", CUSTOMERS);
        Map<Vertex, Integer> outbound = new HashMap<>();
        Vertex assembled = NestDag.attach(dag,
                NestTopology.compile("p", "doc", body, tables::get),
                "doc", "o", "doc",
                alias -> List.of(byAlias.get(alias)),
                new NestBinding(tables::get, HeapNestStores.onHeap(), (from, released) -> { }),
                vertex -> outbound.merge(vertex, 1, Integer::sum) - 1,
                new NestFrontier(AXES, alias -> List.of(List.of(chainOfAlias.get(alias)))));

        Vertex sink = dag.newVertex("sink", SinkProcessor.metaSupplier(
                (SupplierEx<SinkWriter>) TakesEverything::new,
                (SinkAckFactory) resolved -> (SinkAck)
                        TheFrontierReachesTheStreamOfRowsDocumentsPointAtTest::record,
                () -> new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN)));
        dag.edge(Edge.from(assembled, outbound.merge(assembled, 1, Integer::sum) - 1)
                .to(sink, 0).distributed());
        job = member.getJet().newJob(dag);
    }

    private static Map<String, Object> row(String a, Object av, String b, Object bv) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(a, av);
        row.put(b, bv);
        return row;
    }

    /** One row a source emits, with the order the engine would have stamped on it. */
    private record Row(Map<String, Object> fields, SourceOrder order) implements Serializable {
    }

    private static ProcessorMetaSupplier source(String stream, List<Row> rows) {
        return source(stream, rows, 0);
    }

    private static ProcessorMetaSupplier source(String stream, List<Row> rows, long holdBackMillis) {
        List<Row> plan = List.copyOf(rows);
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(
                (SupplierEx<Processor>) () -> new RowsThenBounds(stream, plan, holdBackMillis)));
    }

    /**
     * Emits its rows, then keeps raising its bound for as long as the job runs - a level only reconsiders a
     * chain when a bound on it arrives, and a source that finished would stop constraining the coalesced
     * bound and jump it to the highest any queue ever reported, which would pass this for the wrong reason.
     */
    private static final class RowsThenBounds extends AbstractProcessor {

        private final String stream;
        private final List<Row> rows;
        private final long holdBackMillis;
        private long startedAt = -1;
        private int next;
        private long bound = FAR_ABOVE;

        RowsThenBounds(String stream, List<Row> rows, long holdBackMillis) {
            this.stream = stream;
            this.rows = rows;
            this.holdBackMillis = holdBackMillis;
        }

        /** Not cooperative so it can pace itself: raising the bound as fast as it can spins the job. */
        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
            if (startedAt < 0) {
                startedAt = System.currentTimeMillis();
            }
            if (next < rows.size() && System.currentTimeMillis() - startedAt < holdBackMillis) {
                return false;
            }
            while (next < rows.size()) {
                Row row = rows.get(next);
                if (!tryEmit(Envelope.insert(next + 1L, stream, row.fields(), null).withOrder(row.order()))) {
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

    /** A sink that confirms everything at once: what is written is not what this test is about. */
    private static final class TakesEverything implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
