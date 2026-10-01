package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.DAG;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.model.BatchSpec;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.ExecutionShape;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A run writes down the plan it is submitted on - the width each node was worked out to run at, for the members
 * that was worked out for, the batch each takes its input in, and which run it is - before it is submitted, and
 * the plan is let go of once the pipeline stops. The plans are kept where any member can read them, whichever
 * member submitted the run.
 */
class ARunWritesDownThePlanItIsSubmittedOnTest {

    private static final String PIPE = "p";
    private static final Instant T0 = Instant.parse("2026-09-26T10:00:00Z");

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
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
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void aStartWritesDownThePlanItsRunIsSubmittedOnAndAStopLetsItGo() {
        RecordingPlans plans = new RecordingPlans();
        EngineLifecycleActuator actuator = new EngineLifecycleActuator(new Engine(member), new PlannedIdle(),
                new NoOpCaptureCoordinator(),
                new NestStateTeardown(member, new InMemoryKeyedStateStore(), new InMemoryNestDeadLetterStore()),
                PipelineActuationOwnership.single(), plans, Clock.fixed(T0, ZoneOffset.UTC));

        actuator.start(PIPE);

        assertThat(plans.recorded).singleElement().satisfies(plan -> {
            assertThat(plan.pipelineId()).isEqualTo(PIPE);
            assertThat(plan.members()).containsExactly("m1", "m2", "m3");
            assertThat(plan.plannedAt()).isEqualTo(T0);
            assertThat(plan.claimGeneration()).as("a single node fences nothing").isNull();
            assertThat(plan.executionGeneration()).isNull();
            assertThat(plan.topologyRevision()).isNull();
            assertThat(plan.nodes()).containsExactly(
                    new ExecutionPlan.Node("orders_src", 1, "node-default", "total-one", 3, null, 1,
                            List.of("requested-one", "source-reads-not-split"), 1024, 0L, List.of("orders_src")),
                    new ExecutionPlan.Node("serve.s", 8, "explicit", "native", 3, 3, 9, List.of("rounded-up"),
                            512, 50L, List.of("route.serve.s", "serve.s"))
                            // Nine writers, a connector each; two batches of 512 each; and a full queue from the
                            // one source processor to each of nine routers, and from each router to each writer
                            // over its two edges: (1 x 9 + 2 x 9 x 9) queues of 1024.
                            .withResources(new ExecutionPlan.Resources(9, "isolated", 9, 9_216L, 175_104L)));
        });
        assertThat(plans.forgotten).isEmpty();

        actuator.stop(PIPE, false);

        assertThat(plans.forgotten).containsExactly(PIPE);
    }

    @Test
    void writersSharingACertifiedConnectorOpenOnePerMemberRunningThem() {
        ExecutionPlan plan = EngineLifecycleActuator.planOf(PIPE, PipelineActuationOwnership.Execution.unfenced(),
                new PlannedIdle().planned(), T0, Map.of("serve.s", "pg"), Set.of("pg"));

        assertThat(plan.nodes()).filteredOn(node -> node.node().equals("serve.s")).singleElement()
                .extracting(ExecutionPlan.Node::resources)
                .isEqualTo(new ExecutionPlan.Resources(9, "shared", 3, 9_216L, 175_104L));
    }

    @Test
    void aSinkRunAsOneProcessorForTheClusterOpensOneConnectorAndTakesItsInputOnOneQueuePerSender() {
        Map<String, NodeParallelism> nodes = new java.util.LinkedHashMap<>();
        nodes.put("step", new NodeParallelism("step", 4, NodeParallelism.Origin.EXPLICIT,
                NodeParallelism.Scope.NATIVE, 2, 2, 4, List.of()));
        nodes.put("serve.s", new NodeParallelism("serve.s", 1, NodeParallelism.Origin.EXPLICIT,
                NodeParallelism.Scope.TOTAL_ONE, 2, null, 1, List.of("requested-one")));
        DagSource.PlannedDag planned = new DagSource.PlannedDag(new DAG(), new ExecutionShape(2, nodes, Map.of()),
                List.of("m1", "m2"), Map.of("serve.s", new BatchSpec(100, "0ms")),
                Map.of("step", List.of("step"), "serve.s", List.of("serve.s")),
                Map.of("serve.s", List.of("step", "gather")));

        ExecutionPlan plan = EngineLifecycleActuator.planOf(PIPE, PipelineActuationOwnership.Execution.unfenced(),
                planned, T0, Map.of("serve.s", "pg"), Set.of("pg"));

        // Four step processors and a gathering vertex no node runs the width of, each with a queue to the one
        // writer: (4 + 1) queues of 1024.
        assertThat(plan.nodes()).filteredOn(node -> node.node().equals("serve.s")).singleElement()
                .extracting(ExecutionPlan.Node::resources)
                .isEqualTo(new ExecutionPlan.Resources(1, "shared", 1, 200L, 5_120L));
        assertThat(plan.nodes()).filteredOn(node -> node.node().equals("step")).singleElement()
                .extracting(ExecutionPlan.Node::resources).as("only a sink is worked out resources for").isNull();
    }

    @Test
    void aFencedRunNamesTheGenerationsItIsHeldToAndTheTopologyItsClaimWasHeldUnder() {
        PipelineActuationOwnership.Execution fenced =
                new PipelineActuationOwnership.Execution(true, new ExecutionFence(PIPE, 3, 7), 11L);

        ExecutionPlan plan = EngineLifecycleActuator.planOf(PIPE, fenced, new PlannedIdle().planned(), T0);

        assertThat(plan.claimGeneration()).isEqualTo(3L);
        assertThat(plan.executionGeneration()).isEqualTo(7L);
        assertThat(plan.topologyRevision()).isEqualTo(11L);
    }

    @Test
    void thePlansAreReadInOneGoFromWhereverTheyWereWritten() {
        HazelcastExecutionPlans plans = new HazelcastExecutionPlans(member);
        ExecutionPlan first = EngineLifecycleActuator.planOf("a", PipelineActuationOwnership.Execution.unfenced(),
                new PlannedIdle().planned(), T0);
        ExecutionPlan second = EngineLifecycleActuator.planOf("b", PipelineActuationOwnership.Execution.unfenced(),
                new PlannedIdle().planned(), T0.plusSeconds(1));

        plans.record(first);
        plans.record(second);

        assertThat(plans.current(List.of("a", "b", "never-ran"))).containsOnly(
                Map.entry("a", first), Map.entry("b", second));

        plans.forget("a");

        assertThat(plans.current(List.of("a", "b"))).containsOnlyKeys("b");
        assertThat(plans.current(List.of())).isEmpty();
        assertThat(plans.last("a"))
                .as("a stopped pipeline has no current plan, and its last one is what its next run is compared with")
                .isEqualTo(first);
        assertThat(plans.last("never-ran")).isNull();
    }

    @Test
    void aStartAfterAnEarlierRunWritesDownTheRunItReplacedAndHowEachWidthMoved() {
        RecordingPlans plans = new RecordingPlans();
        plans.last = new ExecutionPlan(PIPE, 2L, 6L, 10L, List.of("m1", "m2", "m3", "m4"),
                List.of(new ExecutionPlan.Node("serve.s", 8, "explicit", "native", 4, 2, 8, List.of(), 512, 50L,
                        List.of("route.serve.s", "serve.s"))),
                T0.minusSeconds(600));
        EngineLifecycleActuator actuator = new EngineLifecycleActuator(new Engine(member), new PlannedIdle(),
                new NoOpCaptureCoordinator(),
                new NestStateTeardown(member, new InMemoryKeyedStateStore(), new InMemoryNestDeadLetterStore()),
                PipelineActuationOwnership.single(), plans, Clock.fixed(T0, ZoneOffset.UTC));

        actuator.start(PIPE);

        assertThat(plans.recorded).singleElement().satisfies(plan -> {
            assertThat(plan.replaces()).isEqualTo(
                    new ExecutionPlan.Replaced(6L, List.of("m1", "m2", "m3", "m4"), T0.minusSeconds(600)));
            assertThat(plan.nodes()).filteredOn(node -> node.node().equals("serve.s")).singleElement()
                    .extracting(ExecutionPlan.Node::change)
                    .isEqualTo(new ExecutionPlan.Change(8, List.of(ExecutionPlan.Change.MEMBERS_CHANGED,
                            ExecutionPlan.Change.CAPABILITY_CHANGED)));
        });
    }

    /** The idle stand-in topology, planned over three members: a source held to one, a sink eight wide. */
    private static final class PlannedIdle implements DagSource {

        private final IdleDagSource idle = new IdleDagSource();

        @Override
        public NestCapacity capacityOf(String pipelineId) {
            return NestCapacity.none();
        }

        @Override
        public DAG dagFor(String pipelineId) {
            return idle.dagFor(pipelineId);
        }

        @Override
        public PlannedDag plannedDagFor(String pipelineId, ExecutionFence fence) {
            PlannedDag planned = planned();
            return new PlannedDag(dagFor(pipelineId), planned.shape(), planned.members(), planned.batches(),
                    planned.vertices(), planned.feeding());
        }

        PlannedDag planned() {
            Map<String, NodeParallelism> nodes = new java.util.LinkedHashMap<>();
            nodes.put("orders_src", new NodeParallelism("orders_src", 1, NodeParallelism.Origin.NODE_DEFAULT,
                    NodeParallelism.Scope.TOTAL_ONE, 3, null, 1, List.of("requested-one", "source-reads-not-split")));
            nodes.put("serve.s", new NodeParallelism("serve.s", 8, NodeParallelism.Origin.EXPLICIT,
                    NodeParallelism.Scope.NATIVE, 3, 3, 9, List.of("rounded-up")));
            return new PlannedDag(new DAG(), new ExecutionShape(3, nodes, Map.of()), List.of("m1", "m2", "m3"),
                    Map.of("serve.s", new BatchSpec(512, "50ms")),
                    Map.of("orders_src", List.of("orders_src"), "serve.s", List.of("route.serve.s", "serve.s")),
                    Map.of("serve.s", List.of("orders_src")));
        }

        @Override
        public Map<String, String> sinkConnectors(String pipelineId) {
            return Map.of("serve.s", "pg");
        }

        @Override
        public List<io.tapstate.core.lifecycle.PipelineStateHolding> stateHeldBy(String pipelineId) {
            return idle.stateHeldBy(pipelineId);
        }
    }

    /**
     * Remembers every plan written down and every pipeline let go of, in order, and answers {@code last} as the plan
     * of the run before.
     */
    private static final class RecordingPlans implements ExecutionPlanRecorder {

        private final List<ExecutionPlan> recorded = new ArrayList<>();
        private final List<String> forgotten = new ArrayList<>();
        private ExecutionPlan last;

        @Override
        public ExecutionPlan last(String pipelineId) {
            return last;
        }

        @Override
        public void record(ExecutionPlan plan) {
            recorded.add(plan);
        }

        @Override
        public void forget(String pipelineId) {
            forgotten.add(pipelineId);
        }
    }
}
