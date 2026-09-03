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
import java.time.Duration;
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
 * <p><b>Neither chain is held, and that is the ruling on both of them.</b> A document waiting for a row it
 * points at is written through to the state as it waits, and the row's arrival is what wakes it - so the
 * frontier crossing the row that put it there loses nothing, and holding it would burn a source's
 * retention window in exchange for nothing. The chain the pointed-at rows arrive on is not held either, for
 * a different reason: a row nobody names owes no document at all. What the two share is that the frontier
 * must cross them; where they differ is what can say so. The waiting document's own row goes out in a later
 * document on its chain. The unnamed row goes out in nothing, ever - so only the vertex that filed it can
 * speak for it, and an implementation that waits for a record instead stops that chain at whichever row
 * some document happened to name, for the life of the job.
 *
 * <p><b>What this case must not be read off is the pinned-duration reading alone.</b> That reading names a
 * chain in either of two states, and one of them - a bound climbed over positions it was never given - is
 * where every chain sits whenever a source's bound runs ahead of its own rows. Asserting a chain is absent
 * from it therefore passes on an implementation whose frontier never moved at all, which is how this case
 * read green while the property did not hold. So what is asserted is the positions actually written down,
 * with the reading kept as the second direction: that having moved, nothing then reports them stopped.
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

        // The position too, not only the document. A document reaches the sink's writer before the sink
        // has confirmed it, and it is the confirming that writes the position down - so waiting on the
        // document alone and then reading what was written down reads a run that has not finished
        // arriving.
        await(() -> !DOCUMENTS.isEmpty()
                && DOCUMENTS.get(DOCUMENTS.size() - 1).get("customer") != null
                && acked().contains(acked(ORDERS, WAITING_ORDER_AT)));

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

        // Waits for the state this case is about to actually arrive, rather than for a length of time
        // that was long enough on a quiet machine. Under a full build the same run has taken several
        // times as long, and a fixed wait that runs out leaves every reading below saying what an
        // unstarted run says.
        //
        // Both positions, not the one that usually lands last. They reach the sink by routes that are
        // not ordered against each other - the order's inside the document that names the row it
        // waited for, the unrelated customer's in a word of its own from the vertex that filed it -
        // so waiting on one and then reading the other reads a run that has not finished arriving.
        // Nothing is given away by waiting for both: a route that never delivers still spends the
        // budget and still fails, naming the position that never came.
        await(() -> acked().contains(acked(ORDERS, MOVING_ORDER_AT))
                && acked().contains(acked(CUSTOMERS, UNRELATED_CUSTOMER_AT)));

        assertThat(DOCUMENTS)
                .describedAs("the control: the run reached the state being asked about. One order's row "
                        + "named a customer that arrived and its document went out; the other names one "
                        + "that never does, so its document is still being waited on. Without this every "
                        + "assertion below is one an unstarted run would also pass")
                .hasSize(1);

        assertThat(acked())
                .describedAs("the unrelated row on the pointed-at chain was let past. No document names "
                        + "it, so no record downstream carries where it sat and nothing but the vertex "
                        + "that filed it can say it is durable. An implementation that leaves it to a "
                        + "record stops this chain at whichever row some document happened to name, for "
                        + "the life of the job, with every document correct and nothing to see but a "
                        + "restart replaying that table from further back every day. "
                        + "What was written down: %s", acked())
                .contains(acked(CUSTOMERS, UNRELATED_CUSTOMER_AT));

        assertThat(acked())
                .describedAs("and the waiting document's own chain was let past its row too, which is "
                        + "the ruling rather than a slip: a document held back for a row it points at is "
                        + "written through to the state as it waits, and the row's arrival wakes it, so "
                        + "a restart above it loses nothing and holding the chain would burn a retention "
                        + "window for no gain. The moving order sits above the waiting one on that chain, "
                        + "so this says the frontier crossed it. What was written down: %s", acked())
                .contains(acked(ORDERS, MOVING_ORDER_AT));

        // Published once a second, and the wait above ends the instant the last of those positions
        // lands - so the newest sample can still be the one taken while this chain was legitimately a
        // few milliseconds behind, which reads here as a frontier that had stopped. Settled first, and
        // the assertion below is still the judge: a frontier that really stopped never settles, so what
        // it reads is the reading that says so.
        settle(() -> new Engine(member).frontierStalls("nest-waiting-frontier").isEmpty());

        assertThat(new Engine(member).frontierStalls("nest-waiting-frontier"))
                .describedAs("and nothing reports a stopped frontier, because nothing has stopped. This "
                        + "is the direction the other two cannot fail in: they would still pass on an "
                        + "implementation that acked these positions and then held the chains anyway, "
                        + "which is what anything reading this to raise an alarm would see")
                .isEmpty();
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
        job = JetJobs.submit(member, dag, "nest-waiting-frontier");
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
     * Emits its rows, then the one bound that stands for them, and then says nothing more.
     *
     * <p><b>The bound stops at the last row read, and that is the contract rather than a convenience.</b>
     * A real source announces what it has read off its ring and nothing beyond it, sends a bound only when
     * it climbs - a repeated one is a torn contract that fails the job - and goes silent when it is idle.
     * A fixture that instead raises its bound for ever, with no rows behind it, models a source that cannot
     * exist, and it made this case undecidable: every chain is then permanently "short of positions it was
     * never given", which is one of the two states the pinned reading is written to report. Both of this
     * case's chains reported pinned under it, so neither the property nor its control could be told from
     * an implementation that stalls everything.
     */
    private static final class RowsThenBounds extends AbstractProcessor {

        private final String stream;
        private final List<Row> rows;
        private int next;
        private SourceOrder read;
        private long announced = Long.MIN_VALUE;

        RowsThenBounds(String stream, List<Row> rows) {
            this.stream = stream;
            this.rows = rows;
        }

        /** Not cooperative so it can pace itself rather than spin a shared thread between empty turns. */
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
                read = row.order();
            }
            if (read != null) {
                long bound = FrontierOrders.pack(stream, read);
                if (bound > announced && !tryEmit(new Watermark(bound, AXES.axisOf(stream)))) {
                    return false;
                }
                announced = Math.max(announced, bound);
            }
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

    /**
     * Waits for what a run is supposed to reach, giving up the moment the job itself ends.
     *
     * <p>The budget is sized for the slowest way this runs rather than for a machine with nothing else on
     * it. Measured: alone this case settles in about six seconds, and under a full build - every module's
     * tests contending for the same cores - the same work took long enough to run out a thirty second
     * budget entirely. A budget that fits the quiet case turns a slow build into a red that reads exactly
     * like a chain being pinned, which is the one thing this case exists to tell apart.
     *
     * <p><b>The budget is the second line, though, not the first.</b> A job that dies stops writing into
     * what this watches without disturbing anything visible here, so a plain deadline spends the whole
     * budget and then names the frontier rather than the death underneath it - measured on this very
     * lane, thirty seconds of waiting on a job that had ended twenty-four milliseconds in.
     */
    private void await(BooleanSupplier reached) {
        JobWatch.until(job, Duration.ofSeconds(90), reached,
                () -> "acked: " + acked() + ", documents: " + List.copyOf(DOCUMENTS));
    }

    /**
     * Waits for what should already be true and then gives up quietly, so that what is asserted is what
     * is actually there rather than a wait's own verdict.
     *
     * <p>For a reading that lags what it reports rather than one that has yet to happen: a published
     * metric is a sample of a moment that has passed, so the reading taken the instant a run reaches its
     * end can be older than the end. Where {@link #await} fails on the budget because what it waits for
     * is the case, this leaves the failing to the assertion, which has the reading to name.
     */
    private static void settle(BooleanSupplier reached) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline && !reached.getAsBoolean()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
        }
    }
}
