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
 * A frontier crossing a whole nest node in a job the engine really runs — a resolver that keeps changes
 * back, then an assembler, then whatever is downstream. Every other bound test drives one level: this is
 * the first that watches a bound travel two of them and a cascading edge between.
 *
 * <p>What it is here to catch is the failure the whole mechanism exists for, and the one no count reports.
 * A child whose parent row has not arrived is taken off the stream and put where no sink can see it. If
 * the bound its upstream reported travels on regardless — which is exactly what the engine does by
 * default — then the frontier is written down above that child, and a restart replays from above it: the
 * change is neither delivered nor replayable, the document is short an element forever, and nothing
 * anywhere says so.
 *
 * <p>The tree is the smallest one with a cascade in it: customers, policies beneath them, claims beneath
 * those. Only the middle level is a resolver, and it is the one holding something.
 *
 * <p>The sources never finish. A finished queue stops constraining the coalesced bound and jumps it to the
 * highest any queue ever reported, which would make these pass for the wrong reason.
 */
class NestHoldsTheFrontierBelowWhatItKeepsTest {

    private static final String CUSTOMERS = "customers";
    private static final String POLICIES = "policies";
    private static final String CLAIMS = "claims";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(CUSTOMERS, POLICIES, CLAIMS));

    /** Where the claim held for a missing policy came from, and the highest bound below it. */
    private static final SourceOrder CLAIM_AT = new SourceOrder(1, 5);
    private static final long BENEATH_THE_CLAIM = FrontierOrders.pack(CLAIMS, CLAIM_AT) - 1;

    /** Far above anything that arrives, so a level passing its edge's bound straight on is unmistakable. */
    private static final long FAR_ABOVE = FrontierOrders.pack(CLAIMS, new SourceOrder(1, 900));

    /** Every bound that got past the nest, as {@code chain:value}. Static: the job runs on the member. */
    private static final List<String> SEEN = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;
    private Job job;

    @BeforeEach
    void startMember() {
        SEEN.clear();
        Config config = new Config();
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
    void noBoundCrossesTheNodeAheadOfAChangeAResolverIsKeepingBack() {
        run(false);

        // The customers chain reaching the top proves the far-above bounds were delivered and acted on,
        // so the claims chain stopping short is a decision rather than a message that never arrived.
        await(() -> SEEN.contains(bound(CUSTOMERS, FAR_ABOVE))
                && SEEN.contains(bound(CLAIMS, BENEATH_THE_CLAIM)));

        assertThat(seen())
                .describedAs("the claim is held for a policy that never arrives, so the frontier may not "
                        + "pass it - and the engine's own default would have carried the upstream's bound "
                        + "straight through")
                .doesNotContain(bound(CLAIMS, FAR_ABOVE));
    }

    @Test
    void theBoundCatchesUpOnceWhatWasHeldIsLetGo() {
        run(true);

        await(() -> SEEN.contains(bound(CLAIMS, FAR_ABOVE)));

        assertThat(seen())
                .describedAs("the policy arrived, so the claim reached its document and nothing here is "
                        + "holding the chain back any more")
                .contains(bound(CLAIMS, FAR_ABOVE));
    }

    // ---- the job under test ------------------------------------------------------------

    /**
     * Customers with policies beneath them and claims beneath those. One claim always arrives; the policy
     * it hangs from arrives only when {@code policyArrives}, which is the whole difference between the two
     * tests — held versus let go.
     */
    private void run(boolean policyArrives) {
        Embed claims = new Embed("cl", Map.of("policy_id", "policy_id"), EmbedAs.ARRAY, "claims",
                List.of("claim_id"), null, null, null);
        Embed policies = new Embed("p", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, "policies",
                List.of("policy_id"), null, null, List.of(claims));
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("c", List.of("customer_id"), null, null, List.of(policies)));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("c", new NestTable(CUSTOMERS, List.of("customer_id")));
        tables.put("p", new NestTable(POLICIES, List.of("policy_id")));
        tables.put("cl", new NestTable(CLAIMS, List.of("claim_id")));

        DAG dag = new DAG();
        Map<String, Vertex> byAlias = new LinkedHashMap<>();
        byAlias.put("c", dag.newVertex(CUSTOMERS, source(CUSTOMERS,
                List.of(new Row(row("customer_id", "C1"), new SourceOrder(1, 1))))));
        byAlias.put("p", dag.newVertex(POLICIES, source(POLICIES, policyArrives
                ? List.of(new Row(row("policy_id", "P1", "customer_id", "C1"), new SourceOrder(1, 6)))
                : List.of())));
        byAlias.put("cl", dag.newVertex(CLAIMS, source(CLAIMS,
                List.of(new Row(row("claim_id", "CL1", "policy_id", "P1"), CLAIM_AT)))));

        Map<String, String> chainOfAlias = Map.of("c", CUSTOMERS, "p", POLICIES, "cl", CLAIMS);
        Map<Vertex, Integer> outbound = new HashMap<>();
        Vertex assembled = NestDag.attach(dag,
                NestTopology.compile("p", "doc", body, tables::get),
                "doc", "c", "doc",
                alias -> List.of(byAlias.get(alias)),
                new NestBinding(tables::get, NestBinding.onHeap(), element -> { }),
                vertex -> outbound.merge(vertex, 1, Integer::sum) - 1,
                new NestFrontier(AXES, alias -> List.of(List.of(chainOfAlias.get(alias)))));

        Vertex collector = dag.newVertex("collector", ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) Collector::new)));
        dag.edge(Edge.from(assembled, outbound.merge(assembled, 1, Integer::sum) - 1)
                .to(collector, 0).distributed());
        job = member.getJet().newJob(dag);
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
     * what makes the second test decidable: a level only reconsiders a chain when a bound on it arrives,
     * so one bound sent before the release would leave nothing to carry the higher answer.
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

    /** Records every bound that got past the assembler, named by the chain it belongs to. */
    private static final class Collector extends AbstractProcessor {

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            return true;
        }

        @Override
        public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            SEEN.add(bound(AXES.chainOn(watermark.key()), watermark.timestamp()));
            return true;
        }
    }

    // ---- reading what got through -------------------------------------------------------

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            row.put((String) fields[i], fields[i + 1]);
        }
        return row;
    }

    private static String bound(String chain, long value) {
        return chain + ":" + value;
    }

    /** What has arrived so far, taken at one instant: the job goes on raising bounds while this reads. */
    private static List<String> seen() {
        synchronized (SEEN) {
            return List.copyOf(SEEN);
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
        // Timing out silently would leave the assertions below reading an empty list as agreement.
        throw new AssertionError("the bounds waited for never arrived; what did: " + List.copyOf(SEEN));
    }
}
