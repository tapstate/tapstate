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
import io.tapstate.runtime.engine.nest.HeapNestStores;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestDag;
import io.tapstate.runtime.engine.nest.NestFrontier;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
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
 * A frontier crossing a whole nest node in a job the engine really runs — a resolver that keeps changes
 * back, then an assembler, then whatever is downstream. Every other bound test drives one level: this is
 * the first that watches a bound travel two of them and a cascading edge between.
 *
 * <p>What it is here to catch is the shape of the answer to "where may the frontier go". A child whose
 * parent row has not arrived is taken off the stream and put in this level's state - and that state is
 * written through to a store as the drain settles, so the child is somewhere it survives a restart from.
 * The frontier may therefore pass it: what the frontier promises is that everything at or below it is
 * either at a sink or held somewhere durable, not that it has been emitted. Keeping the bound beneath a
 * held child instead would pin the source's read offset on a row that may never come, and burn the
 * source's retention window while every count reads healthy.
 *
 * <p>The discriminating half is the second assertion: the bound must go past <em>while the child is still
 * held</em>. A level that let the child go - dead-lettered it, timed it out - would move the bound too,
 * and would be losing a row to do it.
 *
 * <p>The tree is the smallest one with a cascade in it: customers, policies beneath them, claims beneath
 * those. Only the middle level is a resolver, and it is the one holding something.
 *
 * <p>The sources never finish. A finished queue stops constraining the coalesced bound and jumps it to the
 * highest any queue ever reported, which would make these pass for the wrong reason.
 */
class NestLetsTheFrontierPastWhatItHoldsDurablyTest {

    private static final String CUSTOMERS = "customers";
    private static final String POLICIES = "policies";
    private static final String CLAIMS = "claims";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(CUSTOMERS, POLICIES, CLAIMS));

    /** Where the claim held for a missing policy came from. */
    private static final SourceOrder CLAIM_AT = new SourceOrder(1, 5);

    /** Far above anything that arrives, so a level passing its edge's bound straight on is unmistakable. */
    private static final long FAR_ABOVE = FrontierOrders.pack(CLAIMS, new SourceOrder(1, 900));

    /** Every bound that got past the nest, as {@code chain:value}. Static: the job runs on the member. */
    private static final List<String> SEEN = Collections.synchronizedList(new ArrayList<>());

    /** Anything a level stopped holding and handed off instead. Empty is the expected reading. */
    private static final List<String> LET_GO = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;
    private Job job;

    @BeforeEach
    void startMember() {
        SEEN.clear();
        LET_GO.clear();
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
    void theBoundCrossesTheNodeAheadOfAChangeAResolverIsStillHolding() {
        run(false);

        // The customers chain reaching the top proves the far-above bounds were delivered and acted on,
        // so the claims chain arriving there is this level's answer rather than an accident of timing.
        await(() -> reached(CUSTOMERS, FAR_ABOVE) && reached(CLAIMS, FAR_ABOVE));

        assertThat(reached(CLAIMS, FAR_ABOVE))
                .describedAs("the claim is held in state that is written through to a store, so it survives "
                        + "a restart on its own and the frontier has no reason to wait beneath it")
                .isTrue();
        assertThat(LET_GO)
                .describedAs("the bound went past while the claim was still held - a level that had thrown "
                        + "the claim away would move the bound too, and would have lost a row to do it")
                .isEmpty();
    }

    @Test
    void theBoundCrossesJustTheSameWhenNothingIsHeldAtAll() {
        run(true);

        await(() -> reached(CLAIMS, FAR_ABOVE));

        assertThat(reached(CLAIMS, FAR_ABOVE))
                .describedAs("the policy arrived, so the claim reached its document; the answer is the same "
                        + "as when it was held, which is the point")
                .isTrue();
        assertThat(LET_GO).isEmpty();
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
                new NestBinding(tables::get, HeapNestStores.onHeap(),
                        (from, released) -> LET_GO.add(from + ":" + released)),
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

    /**
     * Whether a bound on {@code chain} has reached {@code atLeast}, rather than whether that exact value was
     * ever republished. A level promises what it has earned and skips the rest: while it is holding a
     * document back it answers with the value beneath what it holds, and by the time it lets go the source
     * has raised its own bound past the one being waited for. Waiting for the exact number would be waiting
     * for a coincidence of timing rather than for the frontier to cross.
     */
    private static boolean reached(String chain, long atLeast) {
        String prefix = chain + ":";
        for (String seen : seen()) {
            if (seen.startsWith(prefix) && Long.parseLong(seen.substring(prefix.length())) >= atLeast) {
                return true;
            }
        }
        return false;
    }

    /** What has arrived so far, taken at one instant: the job goes on raising bounds while this reads. */
    private static List<String> seen() {
        synchronized (SEEN) {
            return List.copyOf(SEEN);
        }
    }

    /** Waits on the running job, so a job that dies is reported as that and not as a slow mechanism. */
    private void await(BooleanSupplier reached) {
        JobWatch.until(job, Duration.ofSeconds(30), reached, () -> String.valueOf(List.copyOf(SEEN)));
    }
}
