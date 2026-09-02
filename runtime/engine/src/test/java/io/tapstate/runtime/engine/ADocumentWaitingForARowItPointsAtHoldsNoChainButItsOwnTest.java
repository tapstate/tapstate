package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
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
import io.tapstate.runtime.engine.nest.HeapNestStores;
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
 * That a document waiting for the row it points at is completed when that row turns up, and that while it
 * waits it holds back no chain but the one its own row arrived on.
 *
 * <p><b>The second half is the one worth a job to witness.</b> A level that documents wait at is a new
 * place for a wait to become permanent, and permanent here has no symptom: the job stays RUNNING, nothing
 * is thrown, no count is out of place, and every document that did assemble is correct. What stops is the
 * durable frontier - the record of how far a restart may skip - and a frontier that has quietly stopped
 * looks exactly like one with nothing to advance past.
 *
 * <p><b>Which chain is asked about is the whole of the discrimination.</b> The document's own row is
 * genuinely held: it went out in no document, and letting a restart skip past it would lose it. The chain
 * the pointed-at rows arrive on is not - filing one of those rows wakes everything pointing at it within
 * the same drain, so nothing about it is ever owed. An implementation that holds that chain too is
 * indistinguishable from this one on every document it produces, and the two differ only in whether a
 * restart of a long-lived job replays from the beginning of a table nobody was waiting on.
 *
 * <p>So the case asserts both directions at once: the chain that must move has moved, and the chain that
 * must not has not. Either one alone passes on an implementation that stalls everything, or on one that
 * acknowledges everything.
 */
class ADocumentWaitingForARowItPointsAtHoldsNoChainButItsOwnTest {

    private static final String ORDERS = "orders";
    private static final String CUSTOMERS = "customers";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(ORDERS, CUSTOMERS));

    /** The order that waits, and the one that goes straight out beside it. */
    private static final SourceOrder WAITING_ORDER_AT = new SourceOrder(1, 1);
    private static final SourceOrder MOVING_ORDER_AT = new SourceOrder(1, 2);

    /** The row the moving order names, and an unrelated one further along the same chain. */
    private static final SourceOrder NAMED_CUSTOMER_AT = new SourceOrder(1, 5);
    private static final SourceOrder UNRELATED_CUSTOMER_AT = new SourceOrder(1, 6);

    /** The row the waiting order points at. */
    private static final String WAITED_FOR = "C-waited-for";

    /** The row the moving order points at, which is what keeps the sink advancing at all. */
    private static final String NAMED = "C-named";

    /** A row on the same chain that nothing points at - the unrelated arrival the frontier must pass. */
    private static final String UNRELATED = "C-unrelated";

    /** Far above anything that arrives, so nothing here is short of a bound to advance under. */
    private static final long FAR_ABOVE = FrontierOrders.pack(CUSTOMERS, new SourceOrder(1, 900));

    /** Every position the sink wrote down, as {@code chain:epoch:seq}. Static: the job runs on the member. */
    private static final List<String> ACKED = Collections.synchronizedList(new ArrayList<>());

    /** The documents the sink was handed. Static for the same reason. */
    private static final List<Map<String, Object>> DOCUMENTS =
            Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;
    private Job job;

    @BeforeEach
    void startMember() {
        ACKED.clear();
        DOCUMENTS.clear();
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
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
            job = null;
        }
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("the document is completed by the row it named, once that row turns up")
    void aDocumentIsFinishedWhenTheRowItPointsAtArrives() {
        run(true);

        await(() -> !DOCUMENTS.isEmpty() && DOCUMENTS.get(DOCUMENTS.size() - 1).get("customer") != null);

        assertThat(DOCUMENTS.get(DOCUMENTS.size() - 1))
                .describedAs("the row arrived after the document that names it, and the document was "
                        + "completed rather than left as it first went out. A document assembled once and "
                        + "never revisited stops at the version with no customer in it, which is a "
                        + "document the source never had and nothing downstream can tell from a real one")
                .containsEntry("customer", Map.of("customer_id", WAITED_FOR, "name", "Ada"));

        assertThat(acked())
                .describedAs("and its own row is let past once it has gone out in a document")
                .contains(acked(ORDERS, WAITING_ORDER_AT));
    }

    @Test
    @DisplayName("while it waits, an unrelated row on the pointed-at chain is still let past")
    void theChainThePointedAtRowsArriveOnKeepsMovingWhileADocumentWaits() {
        run(false);

        // Wait on the thing known to happen, so what follows reads a settled run rather than a race.
        await(() -> ACKED.contains(acked(ORDERS, MOVING_ORDER_AT)));
        // Long enough that a chain which is pinned has been pinned measurably, and one that is not has
        // had every chance to say so: the sources raise their bounds every few milliseconds throughout.
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(4));

        Map<String, Long> pinned = new Engine(member).frontierStalls("nest-waiting-frontier");

        assertThat(pinned)
                .describedAs("the chain the pointed-at rows arrive on is not pinned, while a document is "
                        + "still waiting for one of its rows. Filing such a row wakes everything pointing "
                        + "at it inside the same drain, so nothing about that chain is ever owed - and a "
                        + "wait that pinned it would stop a restart from skipping a table nobody was "
                        + "waiting on, with the job running, every document correct and nothing to see. "
                        + "What is pinned: %s", pinned)
                .doesNotContainKey(CUSTOMERS);

        assertThat(pinned)
                .describedAs("the control, and it is what stops the assertion above from passing on a run "
                        + "where nothing was waiting at all: the waiting order's own chain is pinned, "
                        + "because its row went out in no document and a restart skipping it would lose "
                        + "it. Both directions are needed - one alone passes on an implementation that "
                        + "pins everything, the other on one that pins nothing")
                .containsKey(ORDERS);
    }

    // ---- the job under test ------------------------------------------------------------

    /**
     * One order pointing at a customer, and a customers chain that carries either the row it names or an
     * unrelated one. That choice is the whole difference between the two cases: in the first the document
     * is completed, in the second it waits for ever while its chain's neighbour goes past.
     */
    private void run(boolean theRowItNamesArrives) {
        Embed customer = new Embed("c", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("o", List.of("order_id"), null, null, List.of(customer)));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("o", new NestTable(ORDERS, List.of("order_id")));
        tables.put("c", new NestTable(CUSTOMERS, List.of("customer_id")));

        // The second order and the row it names exist only in the waiting case, and only so that the sink
        // is advancing at all: the frontier is written down when records are confirmed, so a run where
        // nothing goes out reports nothing and says nothing about what is or is not being held.
        List<Row> orders = theRowItNamesArrives
                ? List.of(new Row(row("order_id", "O1", "cust_ref", WAITED_FOR), WAITING_ORDER_AT))
                : List.of(new Row(row("order_id", "O1", "cust_ref", WAITED_FOR), WAITING_ORDER_AT),
                        new Row(row("order_id", "O2", "cust_ref", NAMED), MOVING_ORDER_AT));
        List<Row> customers = theRowItNamesArrives
                ? List.of(new Row(row("customer_id", WAITED_FOR, "name", "Ada"), NAMED_CUSTOMER_AT))
                : List.of(new Row(row("customer_id", NAMED, "name", "Grace"), NAMED_CUSTOMER_AT),
                        new Row(row("customer_id", UNRELATED, "name", "Nobody"), UNRELATED_CUSTOMER_AT));

        DAG dag = new DAG();
        Map<String, Vertex> byAlias = new LinkedHashMap<>();
        byAlias.put("o", dag.newVertex(ORDERS, source(ORDERS, orders)));
        byAlias.put("c", dag.newVertex(CUSTOMERS, source(CUSTOMERS, customers)));

        Map<String, String> chainOfAlias = Map.of("o", ORDERS, "c", CUSTOMERS);
        Map<Vertex, Integer> outbound = new HashMap<>();
        Vertex assembled = NestDag.attach(dag,
                NestTopology.compile("p", "doc", body, tables::get),
                "doc", "o", "doc",
                alias -> List.of(byAlias.get(alias)),
                new NestBinding(tables::get, HeapNestStores.onHeap(), (from, released) -> { }),
                vertex -> outbound.merge(vertex, 1, Integer::sum) - 1,
                new NestFrontier(AXES, alias -> List.of(List.of(chainOfAlias.get(alias)))));

        Vertex sink = dag.newVertex("sink", SinkProcessor.metaSupplier(
                (SupplierEx<SinkWriter>) CollectingSinkWriter::new,
                (SinkAckFactory) resolved ->
                        (SinkAck) ADocumentWaitingForARowItPointsAtHoldsNoChainButItsOwnTest::record,
                () -> new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN)));
        dag.edge(Edge.from(assembled, outbound.merge(assembled, 1, Integer::sum) - 1)
                .to(sink, 0).distributed());
        job = member.getJet().newJob(dag, new JobConfig().setName("nest-waiting-frontier"));
    }

    /** One row a source emits, with the order the engine would have stamped on it. */
    private record Row(Map<String, Object> fields, SourceOrder order) implements java.io.Serializable {
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
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
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
