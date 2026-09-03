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
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
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
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A root that tracks its own key changes compiles a second edge for its own stream, and the level may say
 * nothing about that chain until <em>every</em> edge carrying it has promised. So the question this answers
 * is structural and has exactly one bad answer: if the twin edge is never given a bound, the root's chain is
 * pinned at whatever it reached before the switch was turned on - for the life of the job, with the job
 * RUNNING, no error counted, and every reading healthy.
 *
 * <p>Nothing smaller can answer it. Which edges a bound is delivered to is the engine's own behaviour, not
 * this code's, and a unit test hands the bounds over itself - so it would be asserting its own choice. The
 * job here is a real one, with sources that never finish: a finished queue stops constraining the coalesced
 * bound and jumps it to the highest anything ever reported, which would make this pass for the wrong reason.
 *
 * <p>The second test is what stops the first from being satisfied by an implementation that simply never
 * holds anything: it runs a rename that really happens, so a whole document is parked between two keys and
 * the hold is taken and then released while the job runs. Measured on the element path before it was fixed,
 * the hold was never let go of at all.
 *
 * <p>The pair is also the positive control for the harness. The untracked run is the same graph with the
 * switch off, and its bound has to cross too - if it did not, a red tracked run would say nothing about
 * tracking.
 */
class NestKeepsTheFrontierMovingWhileARootChangesKeyTest {

    private static final String CUSTOMERS = "customers";
    private static final String POLICIES = "policies";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(CUSTOMERS, POLICIES));

    /** Far above anything that arrives, so a chain that crossed is unmistakable from one that stalled. */
    private static final long FAR_ABOVE = FrontierOrders.pack(CUSTOMERS, new SourceOrder(1, 900));

    /** Every bound that got past the nest, as {@code chain:value}. Static: the job runs on the member. */
    private static final List<String> SEEN = Collections.synchronizedList(new ArrayList<>());

    /** Every row that got past the nest, as {@code op:customer_id:policyCount}. */
    private static final List<String> DOCUMENTS = Collections.synchronizedList(new ArrayList<>());

    /** What the member is configured with, and so the default parallelism every vertex is left at. */
    private static final int COOPERATIVE_THREADS = 4;

    private HazelcastInstance member;
    private Job job;

    /** The vertex the nest compiled to, kept so a test can ask what the graph settled about it. */
    private Vertex assembled;

    @BeforeEach
    void startMember() {
        SEEN.clear();
        DOCUMENTS.clear();
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(COOPERATIVE_THREADS);
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

    /** The positive control: the same graph with the switch off, so a red tracked run means tracking. */
    @Test
    void theBoundCrossesANestWhoseRootDoesNotTrackItsKey() {
        run(false, false);

        await(() -> reached(CUSTOMERS, FAR_ABOVE));

        assertThat(reached(CUSTOMERS, FAR_ABOVE)).isTrue();
    }

    @Test
    void theBoundCrossesJustTheSameOnceTheRootTracksItsKey() {
        run(true, false);

        await(() -> reached(CUSTOMERS, FAR_ABOVE));

        assertThat(reached(CUSTOMERS, FAR_ABOVE))
                .describedAs("the root's own stream now arrives on two edges, and the level may say nothing "
                        + "about that chain until both have promised - a twin nobody sends a bound to pins "
                        + "the chain for the life of the job with nothing anywhere reporting it")
                .isTrue();
    }

    /**
     * The premise the test below rests on, asserted rather than assumed: the graph does not pin the vertex
     * that assembles documents to one instance, so the key being emptied and the key being filled are worked
     * by different ones. Every other case covering a hand-over drives processors by hand, and a hand-driven
     * pair is a pair the test chose - pinned to one instance here, the move would become a local rearrangement
     * and this whole file would be green on an implementation that cannot cross an instance at all.
     */
    @Test
    void theVertexHoldingDocumentsIsNotPinnedToOneInstance() {
        run(true, true);

        assertThat(assembled.getLocalParallelism())
                .describedAs("left at the member's own default - a graph that pinned this to one would make "
                        + "every hand-over local, and every case covering one would be testing nothing")
                .isNotEqualTo(1);
        assertThat(COOPERATIVE_THREADS)
                .describedAs("and the default it is left at really is more than one on this member")
                .isGreaterThan(1);
    }

    @Test
    void theBoundGoesOnCrossingThroughARenameThatReallyHappens() {
        run(true, true);

        // Waits for the document to be complete rather than merely to exist: the arriving half may render
        // before the tree has been parked for it, and the version that matters is the one after it lands.
        await(() -> reached(CUSTOMERS, FAR_ABOVE) && "INSERT:C2:1".equals(documentFor("C2")));

        assertThat(reached(CUSTOMERS, FAR_ABOVE))
                .describedAs("a whole document was parked between two keys and the hold was let go of again; "
                        + "a hold never released leaves the chain pinned at the rename for good")
                .isTrue();
        assertThat(DOCUMENTS)
                .describedAs("the key the source no longer has is removed downstream")
                .contains("DELETE:C1:0");
        assertThat(documentFor("C2"))
                .describedAs("and the tree that was under the old key arrived under the new one")
                .isEqualTo("INSERT:C2:1");
    }

    // ---- the job under test ------------------------------------------------------------

    /**
     * One customer with one policy beneath it. With {@code renames} the customer's own key is edited part way
     * through, which is the whole of what the last test adds.
     */
    private void run(boolean tracked, boolean renames) {
        Embed policies = new Embed("p", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, "policies",
                List.of("policy_id"), null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("c", List.of("customer_id"), null, tracked ? Boolean.TRUE : null,
                        List.of(policies)));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("c", new NestTable(CUSTOMERS, List.of("customer_id")));
        tables.put("p", new NestTable(POLICIES, List.of("policy_id")));

        List<Row> customers = new ArrayList<>();
        customers.add(Row.insert(row("customer_id", "C1", "name", "n"), new SourceOrder(1, 1)));
        if (renames) {
            // Held back until the policy is really in the document. The two tables are two chains and
            // nothing orders one against the other, so "the tree was under the old key when its key
            // changed" is a precondition to establish rather than to hope for - and a rename that beat the
            // policy there would be testing the no-cascade case instead, which is a different contract.
            customers.add(Row.update(row("customer_id", "C1", "name", "n"),
                    row("customer_id", "C2", "name", "n"), new SourceOrder(1, 5)).onceC1Holds());
        }

        DAG dag = new DAG();
        Map<String, Vertex> byAlias = new LinkedHashMap<>();
        byAlias.put("c", dag.newVertex(CUSTOMERS, source(CUSTOMERS, customers)));
        byAlias.put("p", dag.newVertex(POLICIES, source(POLICIES,
                List.of(Row.insert(row("policy_id", "P1", "customer_id", "C1"), new SourceOrder(1, 2))))));

        Map<String, String> chainOfAlias = Map.of("c", CUSTOMERS, "p", POLICIES);
        Map<Vertex, Integer> outbound = new HashMap<>();
        assembled = NestDag.attach(dag,
                NestTopology.compile("p", "doc", body, tables::get),
                "doc", "c", "doc",
                alias -> List.of(byAlias.get(alias)),
                new NestBinding(tables::get, HeapNestStores.onHeap(), (from, released) -> { }),
                vertex -> outbound.merge(vertex, 1, Integer::sum) - 1,
                new NestFrontier(AXES, alias -> List.of(List.of(chainOfAlias.get(alias)))));

        Vertex collector = dag.newVertex("collector", ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) Collector::new)));
        dag.edge(Edge.from(assembled, outbound.merge(assembled, 1, Integer::sum) - 1)
                .to(collector, 0).distributed());
        job = member.getJet().newJob(dag);
    }

    /**
     * One row a source emits, with the order the engine would have stamped on it. {@code held} means it
     * waits for the document under C1 to be holding its policy before it goes - the one way this harness has
     * of ordering two chains that the engine deliberately does not order.
     */
    private record Row(Op op, Map<String, Object> before, Map<String, Object> after, SourceOrder order,
            boolean held) implements Serializable {

        static Row insert(Map<String, Object> after, SourceOrder order) {
            return new Row(Op.INSERT, null, after, order, false);
        }

        static Row update(Map<String, Object> before, Map<String, Object> after, SourceOrder order) {
            return new Row(Op.UPDATE, before, after, order, false);
        }

        Row onceC1Holds() {
            return new Row(op, before, after, order, true);
        }

        boolean mayGo() {
            String latest = held ? documentFor("C1") : null;
            return !held || (latest != null && !latest.endsWith(":0"));
        }

        Envelope asEvent(long ts, String stream) {
            return (op == Op.INSERT
                    ? Envelope.insert(ts, stream, after, null)
                    : Envelope.update(ts, stream, before, after, null)).withOrder(order);
        }
    }

    private static ProcessorMetaSupplier source(String stream, List<Row> rows) {
        List<Row> plan = List.copyOf(rows);
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new RowsThenBounds(stream, plan)));
    }

    /**
     * Emits its rows, then keeps raising its bound for as long as the job runs. Raising it repeatedly is what
     * makes this decidable: a level only reconsiders a chain when a bound on it arrives, so one bound sent
     * before a hold is released would leave nothing to carry the higher answer.
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
            while (next < rows.size() && rows.get(next).mayGo()) {
                if (!tryEmit(rows.get(next).asEvent(next + 1L, stream))) {
                    return false;
                }
                next++;
            }
            // Not one bound until every row is out. A source that published a bound and then emitted a row
            // beneath it would be lowering a position it had already reported, which the engine treats as
            // the invariant violation it is and tears the job down for - so a source held back for a moment
            // has to hold its bounds back with it, exactly as a real one does.
            if (next < rows.size()) {
                return false;
            }
            if (!tryEmit(new Watermark(bound, AXES.axisOf(stream)))) {
                return false;
            }
            bound++;
            // Never finishes: a completed queue stops constraining the coalesced bound.
            return false;
        }
    }

    /** Records every bound and every document that got past the assembler. */
    private static final class Collector extends AbstractProcessor {

        @Override
        @SuppressWarnings("unchecked")
        protected boolean tryProcess(int ordinal, Object item) {
            Envelope event = (Envelope) item;
            Map<String, Object> row = event.after() != null ? event.after() : event.before();
            Object policies = row == null ? null : row.get("policies");
            DOCUMENTS.add(event.op() + ":" + (row == null ? null : row.get("customer_id")) + ":"
                    + (policies instanceof List<?> held ? held.size() : 0));
            return true;
        }

        @Override
        public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            SEEN.add(AXES.chainOn(watermark.key()) + ":" + watermark.timestamp());
            return true;
        }
    }

    // ---- reading what got through -------------------------------------------------------

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> built = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            built.put((String) fields[i], fields[i + 1]);
        }
        return built;
    }

    /**
     * The last thing said about {@code customerId}, or null while nothing has been. The last rather than any:
     * a document is re-sent whenever it changes, so an earlier version arriving is not what is being asked.
     */
    private static String documentFor(String customerId) {
        String latest = null;
        synchronized (DOCUMENTS) {
            for (String seen : DOCUMENTS) {
                if (seen.contains(":" + customerId + ":")) {
                    latest = seen;
                }
            }
        }
        return latest;
    }

    /**
     * Whether a bound on {@code chain} has reached {@code atLeast}, rather than whether that exact value was
     * ever republished. A level promises what it has earned and skips the rest, so waiting for an exact
     * number would be waiting for a coincidence of timing rather than for the frontier to cross.
     */
    private static boolean reached(String chain, long atLeast) {
        String prefix = chain + ":";
        synchronized (SEEN) {
            for (String seen : SEEN) {
                if (seen.startsWith(prefix) && Long.parseLong(seen.substring(prefix.length())) >= atLeast) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Waits on the running job, so a job that dies is reported as that and not as a slow mechanism. */
    private void await(BooleanSupplier reached) {
        JobWatch.until(job, Duration.ofSeconds(60), reached, () -> "bounds " + List.copyOf(SEEN) + "; documents " + List.copyOf(DOCUMENTS));
    }
}
