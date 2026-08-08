package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
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
import io.tapstate.runtime.engine.ChainAxes;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A frontier crossing the vertex a nest node puts in front of an alias that resolves to several
 * producers. That vertex exists so the node still sees one edge per stream, and it is the only place in
 * the graph that knows which chain arrives on which of those producers' edges: everything above it sees
 * the one gathered edge and could never tell them apart.
 *
 * <p>Which is why what it is told matters. Given the alias's total set of chains it would wait, on every
 * one of its edges, for a chain that only ever arrives on one of them - and promise nothing, ever, with
 * nothing thrown and nothing logged. Given what each edge really carries it promises the lowest of them,
 * which is what the levels behind it are waiting for.
 */
class NestGatheringPassesOnEveryChainsBoundTest {

    private static final String LEFT = "customers";
    private static final String RIGHT = "customers_eu";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(LEFT, RIGHT));

    /** Every bound that got past the gathering, as {@code chain:value}. */
    private static final List<String> BOUNDS = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;
    private Job job;

    @BeforeEach
    void startMember() {
        BOUNDS.clear();
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
    void carriesOnTheBoundOfEachProducerOfTheAlias() {
        DAG dag = new DAG();
        Vertex left = dag.newVertex("left", sender(new Bound(AXES.axisOf(LEFT), 100))).localParallelism(1);
        Vertex right = dag.newVertex("right", sender(new Bound(AXES.axisOf(RIGHT), 200))).localParallelism(1);

        // A root with nothing embedded: the node is one gathering vertex and no assembly at all, which is
        // the smallest graph where an alias's several producers meet.
        NestTopology topology = NestTopology.compile("p", "doc",
                nest("customer", List.of("customer_id")), tables());
        Vertex gathered = NestDag.attach(dag, topology, "doc", "customer", "doc",
                alias -> List.of(left, right), null, vertex -> 0,
                new NestFrontier(AXES, alias -> List.of(List.of(LEFT), List.of(RIGHT))));

        Vertex collector = dag.newVertex("collector",
                ProcessorSupplier.of((SupplierEx<Processor>) Collector::new)).localParallelism(1);
        dag.edge(Edge.between(gathered, collector));
        job = member.getJet().newJob(dag);

        await(() -> BOUNDS.contains(bound(LEFT, 100)) && BOUNDS.contains(bound(RIGHT, 200)));

        assertThat(BOUNDS)
                .describedAs("told the alias's whole set instead, each edge waits for the chain that only "
                        + "reaches the other one, and nothing is ever promised again")
                .contains(bound(LEFT, 100), bound(RIGHT, 200));
    }

    /** One bound a sender puts on its edge: a value on one chain's axis. */
    private record Bound(byte axis, long value) implements Serializable {
    }

    private static ProcessorSupplier sender(Bound bound) {
        return ProcessorSupplier.of((SupplierEx<Processor>) () -> new Sender(bound));
    }

    /** Emits its bound and then stays alive without ever finishing. */
    private static final class Sender extends AbstractProcessor {

        private final Bound bound;
        private boolean sent;

        Sender(Bound bound) {
            this.bound = bound;
        }

        @Override
        public boolean complete() {
            if (!sent) {
                if (!tryEmit(new Watermark(bound.value(), bound.axis()))) {
                    return false;
                }
                sent = true;
            }
            // Never finishes: a completed queue stops constraining the coalesced value.
            return false;
        }
    }

    /** Records every bound that got past the gathering, named by the chain it belongs to. */
    private static final class Collector extends AbstractProcessor {

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            return true;
        }

        @Override
        public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            BOUNDS.add(bound(AXES.chainOn(watermark.key()), watermark.timestamp()));
            return true;
        }
    }

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
        // Timing out silently would leave the assertion below reading an empty list as agreement.
        throw new AssertionError("the bounds waited for never arrived; what did: " + List.copyOf(BOUNDS));
    }
}
