package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.spi.store.PipelineIntentScope;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

/** Exercises actual serialized map values and CAS, including callbacks held by an earlier run. */
class HazelcastExecutionPlansTest {
    private static HazelcastInstance member;
    @BeforeAll static void start() {
        var config = new Config().setClusterName("plans-" + UUID.randomUUID());
        config.getNetworkConfig().setPort(0).getJoin().getMulticastConfig().setEnabled(false);
        config.setProperty("hazelcast.operation.thread.count", "2");
        config.setProperty("hazelcast.operation.generic.thread.count", "2");
        config.setProperty("hazelcast.io.input.thread.count", "1");
        config.setProperty("hazelcast.io.output.thread.count", "1");
        config.setProperty("hazelcast.partition.count", "17");
        config.setProperty("hazelcast.logging.type", "none");
        member = Hazelcast.newHazelcastInstance(config);
    }
    @AfterAll static void stop() { if (member != null) member.shutdown(); }
    @Test void staleRunCannotOverwriteOrForgetItsSuccessorAcrossSerialization() {
        var plans = new HazelcastExecutionPlans(member);
        var scope = new PipelineIntentScope("cluster", 1, "incarnation", "pipeline", 1);
        var old = new ExecutionFence("pipeline", 1, 1, scope, "node", "boot", 1L);
        var next = new ExecutionFence("pipeline", 1, 2, scope, "node", "boot", 1L);
        var oldRecorder = plans.forRun(old, () -> {}); var nextRecorder = plans.forRun(next, () -> {});
        var first = plan(1); var second = plan(2);
        oldRecorder.record(first); nextRecorder.record(second);
        assertThat(plans.current(List.of("pipeline"))).containsEntry("pipeline", second);
        assertThatThrownBy(() -> oldRecorder.record(first)).isInstanceOf(TapstateException.class);
        oldRecorder.forget("pipeline");
        assertThat(plans.current(List.of("pipeline"))).containsEntry("pipeline", second);
        assertThat(nextRecorder.last("pipeline")).isEqualTo(second);
        nextRecorder.forget("pipeline");
        assertThat(plans.current(List.of("pipeline"))).isEmpty();
        assertThat(plans.last("pipeline")).isEqualTo(second);
        assertThatThrownBy(() -> nextRecorder.record(second)).isInstanceOf(TapstateException.class);
        var foreign = new ExecutionFence("pipeline", 1, 3,
                new PipelineIntentScope("cluster", 1, "other-incarnation", "pipeline", 1), "node", "boot", 1L);
        assertThatThrownBy(() -> plans.forRun(foreign, () -> {}).record(plan(3))).isInstanceOf(TapstateException.class);
        assertThat(plans.last("pipeline")).isEqualTo(second);
        assertThat(plans.current(List.of("pipeline"))).isEmpty();
        assertThatThrownBy(() -> oldRecorder.record(first)).isInstanceOf(TapstateException.class);
        assertThat(plans.current(List.of("pipeline"))).isEmpty();
    }
    private static ExecutionPlan plan(long execution) {
        return new ExecutionPlan("pipeline", 1L, execution, 1L, List.of("node"), List.of(), Instant.EPOCH.plusSeconds(execution));
    }
}
