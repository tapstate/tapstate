package io.tapstate.app;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JobConfig;
import io.tapstate.core.lifecycle.ProcessorRuntimeContext;
import io.tapstate.runtime.engine.NativeExecutionStartup;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class NativeProcessorObservationTest {
    @Test void metricIdsAreMatchedInTheEnginesOwnFormatAndOlderRunsCannotSupplyContexts() {
        Job job = mock(Job.class);
        when(job.getId()).thenReturn(42L);
        when(job.getName()).thenReturn("p");
        when(job.getConfig()).thenReturn(new JobConfig().setArgument(NativeExecutionStartup.CLAIM_ARGUMENT, 2L)
                .setArgument(NativeExecutionStartup.EXECUTION_ARGUMENT, 7L)
                .setArgument(NativeExecutionStartup.PROFILE_ARGUMENT, 3L));
        var context = new ProcessorRuntimeContext("p", "v", "42", "43", 2, 7, 3, "a", "boot", "uuid", "a:5701",
                0, 1, 1, 2, 2, 1, Instant.parse("2026-10-10T06:00:00Z"));
        var evidence = new NativeExecutionStartup.Evidence(2, 7, 3, "42", "43", Set.of("v"), Map.of("v", 2), Map.of("v:1", context));

        assertThat(HazelcastLivePipelineRuns.nativeContexts(job, Util.idToString(43), evidence)).containsExactly(context);
        assertThat(HazelcastLivePipelineRuns.nativeContexts(job, Util.idToString(44), evidence)).isEmpty();
        job.getConfig().setArgument(NativeExecutionStartup.EXECUTION_ARGUMENT, 8L);
        assertThat(HazelcastLivePipelineRuns.nativeContexts(job, Util.idToString(43), evidence)).isEmpty();
    }
    @Test void nativeScopeRequiresTheActualJobAndEveryMeasuredExecutionToAgree() {
        Job job = mock(Job.class);
        when(job.getId()).thenReturn(42L);
        when(job.getName()).thenReturn("p");
        when(job.getConfig()).thenReturn(new JobConfig().setArgument(NativeExecutionStartup.CLAIM_ARGUMENT, 2L)
                .setArgument(NativeExecutionStartup.EXECUTION_ARGUMENT, 7L)
                .setArgument(NativeExecutionStartup.PROFILE_ARGUMENT, 3L));
        var context = new ProcessorRuntimeContext("p", "v", "42", "43", 2, 7, 3, "a", "boot",
                "8b0a1e6e-0000-4000-8000-00000000000a", "a:5701", 0, 0, 0, 1, 1, 1,
                Instant.parse("2026-10-10T06:00:00Z"));
        var evidence = new NativeExecutionStartup.Evidence(2, 7, 3, "42", "43", Set.of("v"), Map.of("v", 1), Map.of("v:0", context));
        var current = metrics("43");
        var measured = HazelcastLivePipelineRuns.runOf("p", current);

        assertThat(HazelcastLivePipelineRuns.nativeScope(job, measured, current, evidence))
                .isEqualTo(new io.tapstate.control.core.LivePipelineRun.NativeExecution("42", "43", Util.idToString(43), 2, 7, 3));
        assertThat(HazelcastLivePipelineRuns.nativeScope(job, measured, metrics("43", "41"), evidence)).isNull();
        var conflicted = com.hazelcast.jet.core.metrics.JobMetrics.of(Map.of(
                com.hazelcast.jet.core.metrics.MetricNames.QUEUES_SIZE,
                java.util.List.of(slotReading("old-member-a"), slotReading("old-member-b"))));
        assertThat(HazelcastLivePipelineRuns.nativeScope(job,
                HazelcastLivePipelineRuns.runOf("p", conflicted), conflicted, evidence)).isNull();
        assertThat(HazelcastLivePipelineRuns.nativeScope(job,
                HazelcastLivePipelineRuns.runOf("p", metrics()), metrics(), evidence)).isNull();
        when(job.getId()).thenReturn(41L);
        assertThat(HazelcastLivePipelineRuns.nativeScope(job, measured, current, evidence)).isNull();
        when(job.getId()).thenReturn(42L);
        job.getConfig().setArgument(NativeExecutionStartup.PROFILE_ARGUMENT, 4L);
        assertThat(HazelcastLivePipelineRuns.nativeScope(job, measured, current, evidence)).isNull();
    }

    @Test void anUntaggedProcessorCannotUseAnotherMeasurementsExecutionScope() {
        Job job = mock(Job.class);
        when(job.getId()).thenReturn(42L);
        when(job.getName()).thenReturn("p");
        when(job.getConfig()).thenReturn(new JobConfig().setArgument(NativeExecutionStartup.CLAIM_ARGUMENT, 2L)
                .setArgument(NativeExecutionStartup.EXECUTION_ARGUMENT, 7L)
                .setArgument(NativeExecutionStartup.PROFILE_ARGUMENT, 3L));
        var context = new ProcessorRuntimeContext("p", "v", "42", "43", 2, 7, 3, "a", "boot",
                "8b0a1e6e-0000-4000-8000-00000000000a", "a:5701", 0, 0, 0, 1, 1, 1,
                Instant.parse("2026-10-10T06:00:00Z"));
        var evidence = new NativeExecutionStartup.Evidence(2, 7, 3, "42", "43", Set.of("v"), Map.of("v", 1), Map.of("v:0", context));
        var taggedGlobal = com.hazelcast.jet.core.metrics.Measurement.of(com.hazelcast.jet.core.metrics.MetricNames.QUEUES_SIZE,
                0, 1000, Map.of(com.hazelcast.jet.core.metrics.MetricTags.EXECUTION, Util.idToString(43)));
        var untaggedProcessor = com.hazelcast.jet.core.metrics.Measurement.of(com.hazelcast.jet.core.metrics.MetricNames.QUEUES_SIZE,
                0, 1000, Map.of(com.hazelcast.jet.core.metrics.MetricTags.VERTEX, "v",
                        com.hazelcast.jet.core.metrics.MetricTags.PROCESSOR, "0",
                        com.hazelcast.jet.core.metrics.MetricTags.MEMBER, "old-member"));
        var collected = com.hazelcast.jet.core.metrics.JobMetrics.of(Map.of(
                com.hazelcast.jet.core.metrics.MetricNames.QUEUES_SIZE, java.util.List.of(taggedGlobal, untaggedProcessor)));
        var measured = HazelcastLivePipelineRuns.runOf("p", collected);
        assertThat(measured.executionId()).isEqualTo(Util.idToString(43));
        assertThat(HazelcastLivePipelineRuns.nativeScope(job, measured, collected, evidence)).isNull();
    }

    private static com.hazelcast.jet.core.metrics.Measurement slotReading(String member) {
        return com.hazelcast.jet.core.metrics.Measurement.of(com.hazelcast.jet.core.metrics.MetricNames.QUEUES_SIZE,
                0, 1000, Map.of(com.hazelcast.jet.core.metrics.MetricTags.VERTEX, "v",
                        com.hazelcast.jet.core.metrics.MetricTags.PROCESSOR, "0",
                        com.hazelcast.jet.core.metrics.MetricTags.MEMBER, member,
                        com.hazelcast.jet.core.metrics.MetricTags.EXECUTION, Util.idToString(43)));
    }

    private static com.hazelcast.jet.core.metrics.JobMetrics metrics(String... executions) {
        var readings = java.util.stream.IntStream.range(0, executions.length).mapToObj(index ->
                com.hazelcast.jet.core.metrics.Measurement.of(com.hazelcast.jet.core.metrics.MetricNames.QUEUES_SIZE,
                        0, 1000, Map.of(com.hazelcast.jet.core.metrics.MetricTags.VERTEX, "v",
                                com.hazelcast.jet.core.metrics.MetricTags.PROCESSOR, Integer.toString(index),
                                com.hazelcast.jet.core.metrics.MetricTags.MEMBER, "8b0a1e6e-0000-4000-8000-00000000000a",
                                com.hazelcast.jet.core.metrics.MetricTags.EXECUTION, Util.idToString(Long.parseLong(executions[index]))))).toList();
        return com.hazelcast.jet.core.metrics.JobMetrics.of(Map.of(com.hazelcast.jet.core.metrics.MetricNames.QUEUES_SIZE, readings));
    }
}
