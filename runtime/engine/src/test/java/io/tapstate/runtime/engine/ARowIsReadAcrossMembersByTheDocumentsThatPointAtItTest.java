package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
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
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestDag;
import io.tapstate.runtime.engine.nest.NestFrontier;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * That a document reads the row it points at when that row lives on another member, and reads it again
 * when the row changes.
 *
 * <p><b>This is a standing guard on a deliberate exception.</b> Every other kind of state in a nest is
 * reached only by events routed to the partition holding it - a discipline that makes a vertex's own
 * state its own business. Resolving a pointed-at row breaks that on purpose: the document is not sent to
 * the row, the row is fetched to the document, and the row is wherever its own identity puts it.
 * Everything this direction renders rests on that fetch working across the cluster.
 *
 * <p><b>One member cannot tell whether it does.</b> There every partition is local, so a fetch that only
 * ever reads what happens to be on this member is word for word a fetch that reads across the cluster -
 * same documents, same counts, same everything. So this runs two members and, before asserting anything
 * about documents, establishes that the row and some of the documents pointing at it really did land on
 * different ones. Without that, a run where they all landed together would pass while proving nothing,
 * and which happens is up to a hash.
 *
 * <p><b>The terminal vertex is this case's own, and the reason is worth stating.</b> The sink a pipeline
 * is built with is pinned to a single processor, which a graph spread over two members cannot route to;
 * that is a real gap and it belongs to whoever makes built pipelines run on a cluster, not to rows being
 * pointed at. Collecting the documents in a vertex of this case's own reaches the question this case
 * asks without standing in for that one.
 */
class ARowIsReadAcrossMembersByTheDocumentsThatPointAtItTest {

    private static final String ORDERS_CHAIN = "orders";
    private static final String CUSTOMERS = "customers";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(ORDERS_CHAIN, CUSTOMERS));

    /** The one row every document points at. */
    private static final String CUSTOMER = "C1";

    /** Enough documents that the partitions holding them fall on both members. */
    private static final int ORDERS = 24;

    private static final String BEFORE = "Ada-before";
    private static final String AFTER = "Grace-after";

    private static final SourceOrder CUSTOMER_AT = new SourceOrder(1, 1);
    private static final SourceOrder EDIT_AT = new SourceOrder(1, 500);

    /** How long after the start the edit is due, so it lands on documents that already exist. */
    private static final long EDIT_DUE_AFTER_MILLIS = 6_000L;

    /** Far above anything that arrives, so nothing here is short of a bound to advance under. */
    private static final long FAR_ABOVE = FrontierOrders.pack(CUSTOMERS, new SourceOrder(1, 9000));

    /** Every position the sink wrote down, as {@code chain:epoch:seq}. Static: the job runs on the member. */
    private static final List<String> ACKED = Collections.synchronizedList(new ArrayList<>());

    /** The documents the sink was handed. Static for the same reason. */
    private static final List<Map<String, Object>> DOCUMENTS =
            Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance first;
    private HazelcastInstance second;
    private Job job;

    @BeforeEach
    void startTwoMembers() {
        ACKED.clear();
        DOCUMENTS.clear();
        String cluster = "nest-across-members-" + System.nanoTime();
        first = Hazelcast.newHazelcastInstance(clustered(cluster));
        second = Hazelcast.newHazelcastInstance(clustered(cluster));
    }

    /** A member that joins others of the same cluster name over the loopback, configured as the app is. */
    private static Config clustered(String cluster) {
        Config config = new Config();
        config.setClusterName(cluster);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(15701).setPortAutoIncrement(true).setPortCount(2);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).setMembers(List.of("127.0.0.1:15701", "127.0.0.1:15702"));
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        // The same registration the assembly root makes: a change is three row images of names to whatever
        // the source had, which nothing zero-configuration will write. Without it every edge between these
        // two members fails on its first event.
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setTypeClass(Envelope.class)
                .setImplementation(new EnvelopeSerializer()));
        return config;
    }

    @AfterEach
    void stopBoth() {
        if (job != null) {
            job.cancel();
            job = null;
        }
        if (second != null) {
            second.shutdown();
        }
        if (first != null) {
            first.shutdown();
        }
    }

    @Test
    @DisplayName("documents held on one member read and re-read a row that lives on the other")
    void everyDocumentFollowsAnEditToARowHeldOnAnotherMember() {
        assertThat(first.getCluster().getMembers())
                .describedAs("both members have to be in one cluster, or this is two separate "
                        + "single-member runs and the whole question does not arise")
                .hasSize(2);

        run();
        await(() -> namesByOrder().size() == ORDERS
                && namesByOrder().values().stream().allMatch(AFTER::equals));

        String lookup = "nest.p.doc.customer";
        String roots = "nest.p.doc.$root";
        boolean rowOnFirst = !first.<Object, Object>getMap(lookup).localKeySet().isEmpty();
        HazelcastInstance elsewhere = rowOnFirst ? second : first;

        assertThat(rowOnFirst ^ !second.<Object, Object>getMap(lookup).localKeySet().isEmpty())
                .describedAs("the pointed-at row is held by exactly one of the two, which is what makes "
                        + "'the other member' a thing that can be named")
                .isTrue();
        assertThat(elsewhere.<Object, Object>getMap(roots).localKeySet())
                .describedAs("with %d documents over two members some of them are held away from the row "
                        + "they point at - and if none were, this run could not say whether the fetch "
                        + "reaches across the cluster or only ever read what was already local", ORDERS)
                .isNotEmpty();

        Map<Object, String> byOrder = namesByOrder();
        assertThat(byOrder)
                .describedAs("every document was assembled, so what follows is about all of them")
                .hasSize(ORDERS);
        assertThat(byOrder.values())
                .describedAs("every one carries the edited value, the ones held away from the row "
                        + "included. A fetch that only reads what is local leaves those with no customer "
                        + "at all; one that reads across the cluster but never again leaves them at %s",
                        BEFORE)
                .containsOnly(AFTER);
    }

    /** The customer name on the last document seen for each order. */
    private static Map<Object, String> namesByOrder() {
        Map<Object, String> latest = new LinkedHashMap<>();
        synchronized (DOCUMENTS) {
            for (Map<String, Object> document : DOCUMENTS) {
                if (document.get("customer") instanceof Map<?, ?> customer) {
                    latest.put(document.get("order_id"), String.valueOf(customer.get("name")));
                }
            }
        }
        return latest;
    }

    // ---- the job under test ------------------------------------------------------------

    /**
     * One order pointing at a customer, and a customers chain that carries either the row it names or an
     * unrelated one. That choice is the whole difference between the two cases: in the first the document
     * is completed, in the second it waits for ever while its chain's neighbour goes past.
     */
    private void run() {
        Embed customer = new Embed("c", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("o", List.of("order_id"), null, null, List.of(customer)));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("o", new NestTable(ORDERS_CHAIN, List.of("order_id")));
        tables.put("c", new NestTable(CUSTOMERS, List.of("customer_id")));

        List<Row> orders = new ArrayList<>();
        for (int i = 1; i <= ORDERS; i++) {
            orders.add(new Row(row("order_id", i, "cust_ref", CUSTOMER), new SourceOrder(1, i)));
        }

        DAG dag = new DAG();
        Map<String, Vertex> byAlias = new LinkedHashMap<>();
        byAlias.put("o", dag.newVertex(ORDERS_CHAIN, source(ORDERS_CHAIN, orders)));
        // The row arrives, then changes well after the documents exist: an edit fed alongside them would
        // say nothing about whether they can be reached again once they are spread over the cluster.
        byAlias.put("c", dag.newVertex(CUSTOMERS, source(CUSTOMERS, List.of(
                new Row(row("customer_id", CUSTOMER, "name", BEFORE), CUSTOMER_AT),
                new Row(row("customer_id", CUSTOMER, "name", AFTER), EDIT_AT, EDIT_DUE_AFTER_MILLIS)))));

        Map<String, String> chainOfAlias = Map.of("o", ORDERS_CHAIN, "c", CUSTOMERS);
        Map<Vertex, Integer> outbound = new HashMap<>();
        Vertex assembled = NestDag.attach(dag,
                NestTopology.compile("p", "doc", body, tables::get),
                "doc", "o", "doc",
                alias -> List.of(byAlias.get(alias)),
                new NestBinding(tables::get, NestBinding.onMap(), (from, released) -> { }),
                vertex -> outbound.merge(vertex, 1, Integer::sum) - 1,
                new NestFrontier(AXES, alias -> List.of(List.of(chainOfAlias.get(alias)))));

        Vertex collector = dag.newVertex("collect",
                ProcessorSupplier.of((SupplierEx<Processor>) Collecting::new));
        dag.edge(Edge.from(assembled, outbound.merge(assembled, 1, Integer::sum) - 1)
                .to(collector, 0).distributed());
        job = JetJobs.submit(first, dag, "nest-across-members");
    }

    /** Keeps every document that reaches it. Runs wherever the graph puts it, which is the point. */
    private static final class Collecting extends AbstractProcessor {

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            if (item instanceof Envelope envelope && envelope.after() != null) {
                DOCUMENTS.add(envelope.after());
            }
            return true;
        }
    }

    /** One row a source emits, with the order the engine would have stamped on it. */
    /** One row a source emits, with the order the engine would have stamped on it and when it is due. */
    private record Row(Map<String, Object> fields, SourceOrder order, long dueAfterMillis)
            implements java.io.Serializable {

        Row(Map<String, Object> fields, SourceOrder order) {
            this(fields, order, 0L);
        }
    }

    private static ProcessorMetaSupplier source(String stream, List<Row> rows) {
        List<Row> plan = List.copyOf(rows);
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new RowsThenBounds(stream, plan)));
    }

    /**
     * Emits its rows, then keeps raising its bound for as long as the job runs. Raising it repeatedly is
     * what makes the waiting case decidable: a level only reconsiders a chain when a bound on it arrives.
     */
    private static final class RowsThenBounds extends AbstractProcessor {

        private final String stream;
        private final List<Row> rows;
        private int next;
        private long startedAt = -1;
        private long bound = FAR_ABOVE;

        RowsThenBounds(String stream, List<Row> rows) {
            this.stream = stream;
            this.rows = rows;
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
            while (next < rows.size()) {
                Row row = rows.get(next);
                if (System.currentTimeMillis() - startedAt < row.dueAfterMillis()) {
                    break;
                }
                if (!tryEmit(Envelope.insert(next + 1L, stream, row.fields(), null).withOrder(row.order()))) {
                    return false;
                }
                next++;
            }
            if (!tryEmit(new Watermark(bound, AXES.axisOf(stream)))) {
                return false;
            }
            bound++;
            // Never finishes: a completed queue stops constraining the coalesced bound.
            return false;
        }
    }

    /** Keeps what it was handed, so the completed document can be read rather than only counted. */
    private static final class CollectingSinkWriter implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            for (Envelope record : records) {
                if (record.after() != null) {
                    DOCUMENTS.add(record.after());
                }
            }
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }

    // ---- reading what got written down --------------------------------------------------

    private static void record(String chain, ChainPosition position) {
        ACKED.add(acked(chain, position.order()));
    }

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            row.put((String) fields[i], fields[i + 1]);
        }
        return row;
    }

    private static String acked(String chain, SourceOrder order) {
        return chain + ":" + order.epoch() + ":" + order.seq();
    }

    /** What has been written down so far, taken at one instant: the job runs on while this reads. */
    private static List<String> acked() {
        synchronized (ACKED) {
            return List.copyOf(ACKED);
        }
    }

    private static void await(BooleanSupplier reached) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (System.nanoTime() < deadline) {
            if (reached.getAsBoolean()) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
        }
        throw new AssertionError("what was waited for never happened; acked: " + List.copyOf(ACKED)
                + ", documents: " + List.copyOf(DOCUMENTS));
    }
}
