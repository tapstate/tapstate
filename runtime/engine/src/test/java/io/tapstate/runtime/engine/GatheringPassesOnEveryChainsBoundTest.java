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
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A frontier crossing the vertex that gathers several streams into one, in a job the engine really runs.
 * A union is one, and so is the merge that gives a nest node a single edge where one of its aliases
 * resolves to several producers. Neither transforms anything, which is exactly why the frontier is easy
 * to get wrong here: a vertex that does nothing looks like it needs no code, and the engine's default
 * behaviour for a bound is silently the wrong one.
 *
 * <p>The shape that breaks it is the shape a gathering exists for — two edges, each reading its own
 * table. The combined callback is only ever delivered for a chain every edge has spoken about, so on this
 * shape it is never delivered at all: the bounds stop dead at the gathering, everything downstream waits
 * for a promise that will not come, and nothing is thrown and nothing is logged. The direction is safe
 * (standing still, not running ahead) which is precisely why it would go unnoticed.
 *
 * <p>The senders never finish. A finished queue stops constraining the coalesced value and jumps it to
 * the highest any queue ever reported, which would make these pass for the wrong reason.
 */
class GatheringPassesOnEveryChainsBoundTest {

    private static final String LEFT_ONLY = "customers";
    private static final String RIGHT_ONLY = "items";
    private static final String BOTH = "orders";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(LEFT_ONLY, RIGHT_ONLY, BOTH));

    /** Every bound that reached the far side of the gathering, as {@code chain:value}. */
    private static final List<String> BOUNDS = Collections.synchronizedList(new ArrayList<>());

    /** Every item that reached the far side of the gathering. */
    private static final List<Object> ITEMS = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;
    private Job job;

    @BeforeEach
    void startMember() {
        BOUNDS.clear();
        ITEMS.clear();
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
    void carriesOnTheBoundOfAChainOnlyOneOfItsEdgesCarries() {
        run();

        await(() -> BOUNDS.contains(bound(LEFT_ONLY, 100)) && BOUNDS.contains(bound(RIGHT_ONLY, 200)));

        assertThat(BOUNDS)
                .describedAs("a gathering left to the engine's default hears about neither chain, and "
                        + "everything downstream waits on a promise that never arrives")
                .contains(bound(LEFT_ONLY, 100), bound(RIGHT_ONLY, 200));
    }

    @Test
    void still_passes_every_item_through_unchanged() {
        run();

        await(() -> ITEMS.size() == 2);

        // Gathering is the whole job of this vertex; taking bounds seriously must not change what it does
        // with the items themselves.
        assertThat(ITEMS).containsExactlyInAnyOrder("from-left", "from-right");
    }

    @Test
    void promises_a_chain_both_edges_carry_exactly_once() {
        runBothCarrying();

        await(() -> BOUNDS.contains(bound(BOTH, 50)));

        // Both edges carry this chain, so the engine does deliver the combined callback here - and a
        // gathering that passed that on as well would put the same value on the axis twice. The engine
        // tears a job down on a bound that fails to climb, so the second one is not a harmless repeat.
        assertThat(BOUNDS).filteredOn(bound(BOTH, 50)::equals).hasSize(1);
        assertThat(job.getStatus().isTerminal())
                .describedAs("a repeated bound fails the job, so a still-running job is part of the claim")
                .isFalse();
    }

    // ---- the job under test ------------------------------------------------------------

    private void run() {
        DAG dag = new DAG();
        // One sender each, as the real graph pins them: several instances would each send the whole plan,
        // which says nothing more about the gathering and makes the item count meaningless.
        Vertex left = dag.newVertex("left",
                senders("from-left", new Bound(AXES.axisOf(LEFT_ONLY), 100))).localParallelism(1);
        Vertex right = dag.newVertex("right",
                senders("from-right", new Bound(AXES.axisOf(RIGHT_ONLY), 200))).localParallelism(1);
        Vertex gathering = dag.newVertex("gathering", PassthroughProcessor.metaSupplier(
                AXES, Map.of(0, List.of(LEFT_ONLY), 1, List.of(RIGHT_ONLY))));
        Vertex collector = dag.newVertex("collector",
                ProcessorSupplier.of((SupplierEx<Processor>) Collector::new)).localParallelism(1);

        dag.edge(Edge.from(left).to(gathering, 0));
        dag.edge(Edge.from(right).to(gathering, 1));
        dag.edge(Edge.between(gathering, collector));
        job = member.getJet().newJob(dag);
    }

    /** Both edges carrying one chain: the shape where the combined callback really is delivered. */
    private void runBothCarrying() {
        DAG dag = new DAG();
        Vertex left = dag.newVertex("left",
                senders("from-left", new Bound(AXES.axisOf(BOTH), 50))).localParallelism(1);
        Vertex right = dag.newVertex("right",
                senders("from-right", new Bound(AXES.axisOf(BOTH), 70))).localParallelism(1);
        Vertex gathering = dag.newVertex("gathering", PassthroughProcessor.metaSupplier(
                AXES, Map.of(0, List.of(BOTH), 1, List.of(BOTH))));
        Vertex collector = dag.newVertex("collector",
                ProcessorSupplier.of((SupplierEx<Processor>) Collector::new)).localParallelism(1);

        dag.edge(Edge.from(left).to(gathering, 0));
        dag.edge(Edge.from(right).to(gathering, 1));
        dag.edge(Edge.between(gathering, collector));
        job = member.getJet().newJob(dag);
    }

    private static ProcessorSupplier senders(String item, Bound... bounds) {
        List<Bound> plan = List.of(bounds);
        return ProcessorSupplier.of((SupplierEx<Processor>) () -> new Sender(item, plan));
    }

    /** One bound a sender puts on its edge: a value on one chain's axis. */
    private record Bound(byte axis, long value) implements Serializable {
    }

    /** Emits one item and its bounds, then stays alive without ever finishing. */
    private static final class Sender extends AbstractProcessor {

        private final String item;
        private final List<Bound> plan;
        private boolean sent;
        private int next;

        Sender(String item, List<Bound> plan) {
            this.item = item;
            this.plan = plan;
        }

        @Override
        public boolean complete() {
            if (!sent) {
                if (!tryEmit(item)) {
                    return false;
                }
                sent = true;
            }
            while (next < plan.size()) {
                Bound bound = plan.get(next);
                if (!tryEmit(new Watermark(bound.value(), bound.axis()))) {
                    return false;
                }
                next++;
            }
            // Never finishes: a completed queue stops constraining the coalesced value.
            return false;
        }
    }

    /** Records every bound and every item that got past the gathering. */
    private static final class Collector extends AbstractProcessor {

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            ITEMS.add(item);
            return true;
        }

        @Override
        public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            BOUNDS.add(bound(AXES.chainOn(watermark.key()), watermark.timestamp()));
            return true;
        }
    }

    // ---- reading what got through -------------------------------------------------------

    private static String bound(String chain, long value) {
        return chain + ":" + value;
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
        throw new AssertionError("what was waited for never arrived; bounds: " + List.copyOf(BOUNDS)
                + ", items: " + List.copyOf(ITEMS));
    }
}
