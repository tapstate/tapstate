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
                            512, 50L, List.of("route.serve.s", "serve.s")));
        });
        assertThat(plans.forgotten).isEmpty();

        actuator.stop(PIPE, false);

        assertThat(plans.forgotten).containsExactly(PIPE);
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
                    planned.vertices());
        }

        PlannedDag planned() {
            Map<String, NodeParallelism> nodes = new java.util.LinkedHashMap<>();
            nodes.put("orders_src", new NodeParallelism("orders_src", 1, NodeParallelism.Origin.NODE_DEFAULT,
                    NodeParallelism.Scope.TOTAL_ONE, 3, null, 1, List.of("requested-one", "source-reads-not-split")));
            nodes.put("serve.s", new NodeParallelism("serve.s", 8, NodeParallelism.Origin.EXPLICIT,
                    NodeParallelism.Scope.NATIVE, 3, 3, 9, List.of("rounded-up")));
            return new PlannedDag(new DAG(), new ExecutionShape(3, nodes, Map.of()), List.of("m1", "m2", "m3"),
                    Map.of("serve.s", new BatchSpec(512, "50ms")),
                    Map.of("orders_src", List.of("orders_src"), "serve.s", List.of("route.serve.s", "serve.s")));
        }

        @Override
        public List<io.tapstate.core.lifecycle.PipelineStateHolding> stateHeldBy(String pipelineId) {
            return idle.stateHeldBy(pipelineId);
        }
    }

    /** Remembers every plan written down and every pipeline let go of, in order. */
    private static final class RecordingPlans implements ExecutionPlanRecorder {

        private final List<ExecutionPlan> recorded = new ArrayList<>();
        private final List<String> forgotten = new ArrayList<>();

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
