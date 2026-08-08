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
import org.junit.jupiter.api.Test;

/**
 * The durable frontier of a job whose sink is fed by a nest, end to end and in a job the engine really
 * runs: sources, a resolver that may keep a change back, an assembler, and a real sink advancing what it
 * has confirmed. The bound tests next door watch a promise travel; this watches what is written down.
 *
 * <p>The two halves of the mechanism only meet here. A document is the only thing that ever leaves an
 * assembler, so unless it goes out saying which chains it drew on and how far, a sink has nothing to
 * advance and a chain that ran through a nest can never be acked at all - the frontier stands still while
 * every count reads healthy, and the source's retention window is what eventually runs out. And the bound
 * is the only thing that decides how far those reports may be believed, because a document reaching the
 * sink says nothing about the changes still held back in an assembler it never passed through.
 *
 * <p>So the pair below is one shape with one difference. The claim is held for a policy that arrives in
 * one and never in the other; the frontier follows in the first and must not move in the second. Both are
 * needed: an advance nobody can hold back is not a frontier, and a frontier that never advances is not
 * one either.
 *
 * <p>The sources never finish. A finished queue stops constraining the coalesced bound and jumps it to
 * the highest any queue ever reported, which would make these pass for the wrong reason.
 */
class NestAdvancesTheDurableFrontierTest {

    private static final String CUSTOMERS = "customers";
    private static final String POLICIES = "policies";
    private static final String CLAIMS = "claims";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(CUSTOMERS, POLICIES, CLAIMS));

    private static final SourceOrder CUSTOMER_AT = new SourceOrder(1, 1);
    private static final SourceOrder CLAIM_AT = new SourceOrder(1, 5);
    private static final SourceOrder POLICY_AT = new SourceOrder(1, 6);

    /** Far above anything that arrives, so nothing here is short of a bound to advance under. */
    private static final long FAR_ABOVE = FrontierOrders.pack(CLAIMS, new SourceOrder(1, 900));

    /** Every position the sink wrote down, as {@code chain:epoch:seq}. Static: the job runs on the member. */
    private static final List<String> ACKED = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;
    private Job job;

    @BeforeEach
    void startMember() {
        ACKED.clear();
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
        // Collect the job's statistics once a second, so a frontier reading is readable well inside a test
        // budget rather than after the five seconds the engine waits by default.
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
        }
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void theFrontierReachesEveryChainADocumentDrewOn() {
        run(true);

        await(() -> ACKED.contains(acked(CLAIMS, CLAIM_AT)));

        assertThat(acked())
                .describedAs("the chains a document was assembled from are exactly the ones it lets the "
                        + "frontier past, and a document reporting only the stream it goes out on would "
                        + "leave all of them unacked for good")
                .contains(acked(CUSTOMERS, CUSTOMER_AT), acked(POLICIES, POLICY_AT), acked(CLAIMS, CLAIM_AT));
    }

    @Test
    void theFrontierStopsShortOfAChangeAResolverIsKeepingBack() {
        run(false);

        // The customer chain being written down proves the sink is advancing at all, so the claims chain
        // standing still below is a decision rather than a job that never got going.
        await(() -> ACKED.contains(acked(CUSTOMERS, CUSTOMER_AT)));

        assertThat(acked())
                .describedAs("the claim is held for a policy that never arrives: it went out in no "
                        + "document, and letting the frontier past it would have a restart replay from "
                        + "above a change that was never delivered")
                .doesNotContain(acked(CLAIMS, CLAIM_AT));
    }

    @Test
    void theDistanceTheFrontierTrailsIsPublishedAsAStatisticOfTheJob() {
        run(true);

        await(() -> ACKED.contains(acked(CLAIMS, CLAIM_AT)));

        // The sink is the only thing that can see this: it holds both the bound and the position it
        // reached, and neither is written anywhere a later reader could compare them. A sink assembled
        // without the gauge advances the frontier exactly as correctly as this one and publishes nothing,
        // so what is asserted is that the sink the builder really wires is the one taking readings.
        Map<String, Long> published = publishedGapsWithin(TimeUnit.SECONDS.toNanos(30));

        assertThat(published)
                .describedAs("a reading per chain the sink advanced, named by that chain")
                .containsOnlyKeys(CUSTOMERS, POLICIES, CLAIMS)
                .allSatisfy((chain, gap) -> assertThat(gap).isNotNegative());
    }

    /** The readings once every chain the sink advanced has one, or the last seen when the budget runs out. */
    private Map<String, Long> publishedGapsWithin(long budgetNanos) {
        long deadline = System.nanoTime() + budgetNanos;
        Map<String, Long> published = publishedGaps();
        while (System.nanoTime() < deadline
                && !published.keySet().containsAll(List.of(CUSTOMERS, POLICIES, CLAIMS))) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
            published = publishedGaps();
        }
        return published;
    }

    /** The frontier readings the running job has collected so far, keyed by chain. */
    private Map<String, Long> publishedGaps() {
        return new Engine(member).frontierGaps(job.getName());
    }

    // ---- the job under test ------------------------------------------------------------

    /**
     * Customers with policies beneath them and claims beneath those, ending in a sink that advances what
     * it has confirmed. One claim always arrives; the policy it hangs from arrives only when
     * {@code policyArrives}, which is the whole difference between the two tests.
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
                List.of(new Row(row("customer_id", "C1"), CUSTOMER_AT)))));
        byAlias.put("p", dag.newVertex(POLICIES, source(POLICIES, policyArrives
                ? List.of(new Row(row("policy_id", "P1", "customer_id", "C1"), POLICY_AT))
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

        Vertex sink = dag.newVertex("sink", SinkProcessor.metaSupplier(
                (SupplierEx<SinkWriter>) TakesEverything::new,
                (SinkAckFactory) resolved -> (SinkAck) NestAdvancesTheDurableFrontierTest::record,
                () -> new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN)));
        dag.edge(Edge.from(assembled, outbound.merge(assembled, 1, Integer::sum) - 1)
                .to(sink, 0).distributed());
        // Named so the run's statistics can be read back the way the read face reads them - by pipeline.
        job = member.getJet().newJob(dag, new JobConfig().setName("nest-frontier"));
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
     * what makes the released case decidable: a level only reconsiders a chain when a bound on it arrives.
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

    /** A sink that confirms everything at once: what is written is not what these tests are about. */
    private static final class TakesEverything implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
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
        // Timing out silently would leave the assertions below reading an empty list as agreement.
        throw new AssertionError("the frontier never reached what was waited for; what it did: "
                + List.copyOf(ACKED));
    }
}
