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
import com.hazelcast.jet.core.JobStatus;
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
import io.tapstate.runtime.engine.nest.NestSettings;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What a level may delay, once the frontier has gone past it. A send window merges versions of a document
 * that is being written to often, and it costs nothing while the frontier is still behind what is being
 * merged. This is the case where it is not.
 *
 * <p><b>The two rules that meet here disagree about one change.</b> A change waiting for an ancestor is
 * deliberately not counted among what a level holds the frontier below: it sits in the level's own state,
 * it comes back out the moment its ancestor arrives, and counting it would pin a source's read offset on a
 * foreign key pointing at a row that never comes. So the bound on its chain goes on climbing past it, which
 * is correct - nothing is lost by passing a change that something is certain to finish.
 *
 * <p>Then the ancestor arrives and the change joins its document, and a document that has changed and not
 * gone out <em>is</em> counted - it is a change that survives a restart and that nothing will ever send
 * again, since no further event is due for that root. The same change has crossed from the set that is not
 * held into the set that is, underneath a bound that was published while it was in the first. Two things
 * follow, and they are the two faces of one defect:
 * <ul>
 *   <li><b>Nothing sends it.</b> A restart resumes above the change, so the source never replays it, and
 *       the document folded into the window is never sent again either. The element is missing from the
 *       document for good, with the job running and no count moving.</li>
 *   <li><b>And the level tears the job down saying the wrong thing.</b> What a level may promise is worked
 *       out from two inputs - what its edges promised and what it is holding - and only the first is an
 *       upstream. A fall caused by the second was reported as an upstream lowering a position it had
 *       already reported, which sends whoever reads it looking upstream, where there is nothing to find.</li>
 * </ul>
 *
 * <p><b>Why this is decidable rather than a race.</b> Every source here is honest: none of them ever
 * reports having passed a row it has not yet emitted. The claim is emitted straight away and the bound on
 * its chain climbs past it truthfully, because the source really has sent everything up to there - the
 * change is late because it is waiting for an ancestor, not because anybody lied about it. The ancestor is
 * held back by giving it a position the source has to climb to before it may say it, so the wait is a
 * consequence of the positions rather than of who ran first. The window is set longer than the run, so a
 * change arriving after the leading edge is folded rather than sometimes folded.
 */
class NestSendsAChangeTheBoundHasPassedRatherThanFoldingItTest {

    private static final String CUSTOMERS = "customers";
    private static final String POLICIES = "policies";
    private static final String CLAIMS = "claims";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(CUSTOMERS, POLICIES, CLAIMS));

    private static final SourceOrder CUSTOMER_AT = new SourceOrder(1, 1);

    /**
     * How many claims are seeded. They are emitted at once and wait from then on for a policy that is a long
     * way off, so every one of them is under a bound that has gone past it by the time it joins its
     * document. The count is what makes a short document readable as a number rather than as an absence.
     */
    private static final int CLAIMS_SEEDED = 8;

    /** Where claim {@code index} came from. */
    private static SourceOrder claimAt(int index) {
        return new SourceOrder(1, index + 1L);
    }

    /**
     * The policy, far enough up its own chain that the source has to keep climbing for a while before it
     * may honestly say it. That climb is the wait, and it is made of positions rather than of a clock.
     */
    private static final SourceOrder POLICY_AT = new SourceOrder(1, 800);

    /** How far the policies source climbs before it says the policy, every step of it below where it sits. */
    private static final int POLICY_HELD_BACK_TICKS = 80;

    /** Longer than the run, so anything after the leading-edge send is folded rather than sent. */
    private static final long LONGER_THAN_THE_RUN = TimeUnit.MINUTES.toMillis(10);

    /** Every position the sink wrote down, as {@code chain:epoch:seq}. Static: the job runs on the member. */
    private static final List<String> ACKED = Collections.synchronizedList(new ArrayList<>());

    /** The most claims any document reaching the sink has carried. Static: the job runs on the member. */
    private static final AtomicInteger MOST_CLAIMS_IN_A_DOCUMENT = new AtomicInteger();

    private HazelcastInstance member;
    private Job job;

    @BeforeEach
    void startMember() {
        ACKED.clear();
        MOST_CLAIMS_IN_A_DOCUMENT.set(0);
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
        }
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void aChangeThatWaitedForItsAncestorGoesOutRatherThanIntoAWindowTheBoundHasPassed() {
        run();

        // The customer being written down proves the job is advancing at all, so anything missing below is a
        // decision this level took rather than a run that never got going.
        await(() -> ACKED.contains(acked(CUSTOMERS, CUSTOMER_AT)) || job.getStatus() == JobStatus.FAILED);
        await(() -> MOST_CLAIMS_IN_A_DOCUMENT.get() >= CLAIMS_SEEDED
                || job.getStatus() == JobStatus.FAILED);

        assertThat(job.getStatus())
                .describedAs("what fell was this level's own hold, with every edge still climbing; read as "
                        + "an upstream lowering a position it tears down a job with nothing wrong upstream "
                        + "at all, and no count anywhere moves to say so")
                .isNotEqualTo(JobStatus.FAILED);
        assertThat(MOST_CLAIMS_IN_A_DOCUMENT.get())
                .describedAs("every claim joined its document once the policy arrived, under a bound "
                        + "already past it: folded into a window from there, no restart replays them and "
                        + "no later version sends them, and the document is short its elements for good")
                .isEqualTo(CLAIMS_SEEDED);
        assertThat(acked())
                .describedAs("and the chains the document drew on are the ones it lets the frontier past")
                .contains(acked(CUSTOMERS, CUSTOMER_AT), acked(CLAIMS, claimAt(CLAIMS_SEEDED - 1)));
    }

    // ---- the job under test ------------------------------------------------------------

    private void run() {
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

        NestTopology topology = NestTopology.compile("p", "doc", body, tables::get);
        NestSettings settings = NestSettings.defaults()
                .withSendWindow(topology.assembler().mapName(), LONGER_THAN_THE_RUN);

        DAG dag = new DAG();
        Map<String, Vertex> byAlias = new LinkedHashMap<>();
        byAlias.put("c", dag.newVertex(CUSTOMERS, source(CUSTOMERS,
                List.of(new Row(row("customer_id", "C1"), CUSTOMER_AT)), 0)));
        // The policy is what the claim is waiting for, and it is said late. Every bound this source says
        // until then is below where the policy sits, so it never claims to have passed a row it is holding.
        byAlias.put("p", dag.newVertex(POLICIES, source(POLICIES,
                List.of(new Row(row("policy_id", "P1", "customer_id", "C1"), POLICY_AT)),
                POLICY_HELD_BACK_TICKS)));
        byAlias.put("cl", dag.newVertex(CLAIMS, source(CLAIMS, claimRows(), 0)));

        Map<String, String> chainOfAlias = Map.of("c", CUSTOMERS, "p", POLICIES, "cl", CLAIMS);
        Map<Vertex, Integer> outbound = new HashMap<>();
        Vertex assembled = NestDag.attach(dag, topology, "doc", "c", "doc",
                alias -> List.of(byAlias.get(alias)),
                new NestBinding(tables::get, HeapNestStores.onHeap(), (from, released) -> { }, settings),
                vertex -> outbound.merge(vertex, 1, Integer::sum) - 1,
                new NestFrontier(AXES, alias -> List.of(List.of(chainOfAlias.get(alias)))));

        Vertex sink = dag.newVertex("sink", SinkProcessor.metaSupplier(
                (SupplierEx<SinkWriter>) TakesEverything::new,
                (SinkAckFactory) resolved ->
                        (SinkAck) NestSendsAChangeTheBoundHasPassedRatherThanFoldingItTest::record,
                () -> new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN)));
        dag.edge(Edge.from(assembled, outbound.merge(assembled, 1, Integer::sum) - 1)
                .to(sink, 0).distributed());
        job = member.getJet().newJob(dag, new JobConfig().setName("nest-window-past-the-bound"));
    }

    private static List<Row> claimRows() {
        List<Row> rows = new ArrayList<>();
        for (int index = 0; index < CLAIMS_SEEDED; index++) {
            rows.add(new Row(row("claim_id", "CL" + index, "policy_id", "P1"), claimAt(index)));
        }
        return rows;
    }

    /** One row a source emits, with the order the engine would have stamped on it. */
    private record Row(Map<String, Object> fields, SourceOrder order) implements java.io.Serializable {
    }

    private static ProcessorMetaSupplier source(String stream, List<Row> rows, int heldBackTicks) {
        List<Row> plan = List.copyOf(rows);
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) ()
                        -> new RowsThenBounds(stream, plan, heldBackTicks)));
    }

    /**
     * A source that never says it has passed a row it has not sent. It climbs a step a turn, stopping one
     * short of the lowest row it is still holding, and moves up to that row's own position as it sends it.
     * A source held back for a while therefore spends that while telling the truth about a chain whose next
     * row is still to come, which is the only thing this test needs of it.
     */
    private static final class RowsThenBounds extends AbstractProcessor {

        private final String stream;
        private final List<Row> rows;
        private final int heldBackTicks;
        private int next;
        private int ticks;
        private long bound;

        RowsThenBounds(String stream, List<Row> rows, int heldBackTicks) {
            this.stream = stream;
            this.rows = rows;
            this.heldBackTicks = heldBackTicks;
            this.bound = FrontierOrders.pack(stream, new SourceOrder(1, 0));
        }

        /** Not cooperative so it can pace itself: raising the bound as fast as it can spins the job. */
        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
            if (ticks >= heldBackTicks) {
                while (next < rows.size()) {
                    Row row = rows.get(next);
                    if (!tryEmit(Envelope.insert(next + 1L, stream, row.fields(), null)
                            .withOrder(row.order()))) {
                        return false;
                    }
                    bound = Math.max(bound, FrontierOrders.pack(stream, row.order()));
                    next++;
                }
            }
            if (!tryEmit(new Watermark(bound, AXES.axisOf(stream)))) {
                return false;
            }
            long ceiling = next < rows.size()
                    ? FrontierOrders.pack(stream, rows.get(next).order()) - 1
                    : Long.MAX_VALUE - 1;
            bound = Math.min(bound + 1, ceiling);
            ticks++;
            // Never finishes: a completed queue stops constraining the coalesced bound.
            return false;
        }
    }

    /**
     * A sink that confirms everything at once and counts what each document carried. What is written is not
     * what this test is about; how much of the document arrived is exactly what it is about.
     */
    private static final class TakesEverything implements SinkWriter {

        @Override
        @SuppressWarnings("unchecked")
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            for (Envelope record : records) {
                if (record.after() == null || !(record.after().get("policies") instanceof List<?> under)) {
                    continue;
                }
                for (Object policy : under) {
                    Object nested = ((Map<String, Object>) policy).get("claims");
                    if (nested instanceof List<?> carried) {
                        MOST_CLAIMS_IN_A_DOCUMENT.accumulateAndGet(carried.size(), Math::max);
                    }
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
        throw new AssertionError("the run never reached what was waited for; the most claims any document "
                + "carried was " + MOST_CLAIMS_IN_A_DOCUMENT.get() + " and what got acked was "
                + List.copyOf(ACKED));
    }
}
