package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import org.junit.jupiter.api.Assumptions;
import static org.assertj.core.api.Assertions.assertThat;

/** Qualifies the exact owned member and read-only native job metric path before performance use. */
@RequiresDocker
class BenchmarkNativeQueueProbeIT {
    @Test
    void actualOwnedBootExposesItsNativeQueueRoster() throws Exception {
        String jar = System.getProperty("tapstate.e2e.benchmark-smoke.jar");
        Assumptions.assumeTrue(jar != null, "explicit immutable artifact required for native queue qualification");
        RealConnectorGate.require("mysql", "postgres", "mongodb");
        var workload = BenchmarkWorkloadDefinitions.byId("copy");
        try (var fork = BenchmarkForkEnvironment.open(workload, Path.of(jar), "native-queue-probe")) {
            fork.runPhase(workload.phases().getFirst(), true);
            try (var probe = new BenchmarkNativeQueueProbe(fork.control(), fork.server().baseUrl().toString(), "tapstate",
                    new com.hazelcast.config.MetricsConfig().getCollectionFrequencySeconds())) {
                var sample = probe.read(workload.pipelineIds().getFirst());
                long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
                while ("UNKNOWN".equals(sample.get("state")) && System.nanoTime() < deadline) {
                    java.util.concurrent.TimeUnit.MILLISECONDS.sleep(250);
                    sample = probe.read(workload.pipelineIds().getFirst());
                }
                assertThat(sample).containsEntry("pipeline", workload.pipelineIds().getFirst());
                assertThat(sample).as("native queue family must actually be available for load qualification")
                        .containsEntry("state", "RECORDED");
                System.out.println("benchmark-native-queue-qualified-sample=" + JsonWriter.write(sample));
            }
        }
    }
}
