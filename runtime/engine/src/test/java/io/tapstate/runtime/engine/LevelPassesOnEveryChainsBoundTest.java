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
import io.tapstate.spi.transform.TransformPort;
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
 * A frontier crossing one level, in a job the engine really runs. The level here holds nothing back, so
 * everything it passes on comes from its edges - which makes it the plainest place to see the two things
 * that go wrong when a level is fed by more than one edge.
 *
 * <p>The first is that a chain only one edge carries is invisible to the combined bound: the engine never
 * delivers that callback for such a chain, without an exception or a log, so a level keeping its books
 * that way would let those chains sit still forever while every other one advanced. The second is the
 * opposite failure - a chain both edges carry must wait for both and then be the lower of the two, since
 * one edge saying it has finished with something says nothing about what is still travelling the other.
 *
 * <p>The senders never finish. A finished queue stops constraining the coalesced value and jumps it to
 * the highest any queue ever reported, which would make these pass for the wrong reason.
 */
class LevelPassesOnEveryChainsBoundTest {

    private static final String LEFT_ONLY = "customers";
    private static final String RIGHT_ONLY = "items";
    private static final String BOTH = "orders";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(LEFT_ONLY, RIGHT_ONLY, BOTH));

    /** Every bound that reached the far side of the level, as {@code chain:value}. */
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
    void carriesOnAChainThatOnlyOneOfItsEdgesEvenSees() {
        run();

        await(() -> SEEN.contains(bound(LEFT_ONLY, 100)) && SEEN.contains(bound(RIGHT_ONLY, 200)));

        assertThat(SEEN)
                .describedAs("a level keeping its books on the combined bound would never hear about "
                        + "either of these, and both chains would stand still with nothing reported")
                .contains(bound(LEFT_ONLY, 100), bound(RIGHT_ONLY, 200));
    }

    @Test
    void waitsForBothEdgesOnAChainTheyBothCarryAndThenPassesOnTheLower() {
        run();

        await(() -> SEEN.contains(bound(BOTH, 50)));

        assertThat(SEEN)
                .describedAs("reaching 50 at all means both edges had spoken, and the higher of the two "
                        + "would have claimed changes still travelling the other edge")
                .doesNotContain(bound(BOTH, 70));
    }

    // ---- the job under test ------------------------------------------------------------

    private void run() {
        DAG dag = new DAG();
        Vertex left = dag.newVertex("left", senders(
                new Bound(AXES.axisOf(LEFT_ONLY), 100), new Bound(AXES.axisOf(BOTH), 50)));
        Vertex right = dag.newVertex("right", senders(
                new Bound(AXES.axisOf(RIGHT_ONLY), 200), new Bound(AXES.axisOf(BOTH), 70)));
        Vertex level = dag.newVertex("level", TransformProcessor.metaSupplier(
                () -> (TransformPort) List::of, AXES,
                Map.of(0, List.of(LEFT_ONLY, BOTH), 1, List.of(RIGHT_ONLY, BOTH))));
        Vertex collector = dag.newVertex("collector",
                ProcessorSupplier.of((SupplierEx<Processor>) Collector::new));

        dag.edge(Edge.from(left).to(level, 0));
        dag.edge(Edge.from(right).to(level, 1));
        dag.edge(Edge.between(level, collector));
        job = member.getJet().newJob(dag);
    }

    private static ProcessorSupplier senders(Bound... bounds) {
        List<Bound> plan = List.of(bounds);
        return ProcessorSupplier.of((SupplierEx<Processor>) () -> new Sender(plan));
    }

    /** One bound a sender puts on its edge: a value on one chain's axis. */
    private record Bound(byte axis, long value) implements Serializable {
    }

    /** Emits its bounds and then stays alive without ever finishing. */
    private static final class Sender extends AbstractProcessor {

        private final List<Bound> plan;
        private int next;

        Sender(List<Bound> plan) {
            this.plan = plan;
        }

        @Override
        public boolean complete() {
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

    /** Records every bound that got past the level, named by the chain it belongs to. */
    private static final class Collector extends AbstractProcessor {

        @Override
        public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            SEEN.add(bound(AXES.chainOn(watermark.key()), watermark.timestamp()));
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
        throw new AssertionError("the bounds waited for never arrived; what did: " + List.copyOf(SEEN));
    }
}
