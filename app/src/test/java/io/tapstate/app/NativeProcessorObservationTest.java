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
}
