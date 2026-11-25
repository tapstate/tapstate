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
        assertThat(oldRecorder.forgetIfPresent("pipeline")).isFalse();
        oldRecorder.forget(first);
        assertThat(plans.current(List.of("pipeline"))).containsEntry("pipeline", second);
        assertThat(nextRecorder.last("pipeline")).isEqualTo(second);
        assertThatThrownBy(() -> nextRecorder.forget(first)).isInstanceOf(TapstateException.class);
        assertThat(plans.current(List.of("pipeline"))).containsEntry("pipeline", second);
        assertThat(nextRecorder.forgetIfPresent("pipeline")).isTrue();
        assertThat(nextRecorder.forgetIfPresent("pipeline")).isFalse();
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
    @Test void explicitlyRetiredPriorStateGenerationIsNotAStillLiveOrNewOwnerPlan() {
        var plans = new HazelcastExecutionPlans(member);
        String id = "terminal-" + UUID.randomUUID();
        var oldScope = new PipelineIntentScope("cluster", 1, "incarnation", id, 1);
        var scope = new PipelineIntentScope("cluster", 1, "incarnation", id, 2);
        var old = new ExecutionFence(id, 2, 2, oldScope, "old", "boot-old", 1L);
        var ceiling = new io.tapstate.spi.store.WorkloadClaimFence(
                new io.tapstate.spi.store.WorkloadClaimKey("cluster", io.tapstate.spi.store.WorkloadClaimType.PIPELINE_ACTUATION, id),
                new io.tapstate.spi.store.WorkloadOwner("current", "boot"), 3, 3, 2);
        var recorder = plans.forRun(old, () -> {});
        recorder.record(new ExecutionPlan(id, 2L, 2L, 1L, List.of("old"), List.of(), Instant.EPOCH));
        assertThat(plans.terminalAt(scope, ceiling)).isFalse();
        assertThat(recorder.forgetIfPresent(id)).isTrue();
        assertThat(plans.terminalAt(scope, ceiling)).isTrue();
        var previousCeiling = new io.tapstate.spi.store.WorkloadClaimFence(ceiling.key(), ceiling.owner(), 1, 1, 1);
        assertThatThrownBy(() -> plans.terminalAt(scope, previousCeiling)).isInstanceOf(TapstateException.class);
        assertThat(plans.terminalAt(new PipelineIntentScope("cluster", 1, "other", id, 2), ceiling)).isFalse();
    }

}
