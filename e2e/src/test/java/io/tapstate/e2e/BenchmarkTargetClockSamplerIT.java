package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

@RequiresDocker
class BenchmarkTargetClockSamplerIT {
    @Test void actualPrimarySamplesRetainOneIdentityAndTheirMeasuredCommandBudget() {
        try (var sampler = BenchmarkTargetClockSampler.open(SharedMongo.replicaSetUrl("benchmark_clock_sampler"))) {
            Await.until("three actual owned primary clock samples", Duration.ofSeconds(5),
                    () -> sampler.samplesRecorded() >= 3, () -> "samples=" + sampler.samplesRecorded());
            sampler.close();
            var evidence = sampler.evidence();
            assertThat(evidence).containsEntry("state", "QUALIFIED_SAMPLED_INTERIOR")
                    .containsEntry("sampleIntervalMillis", 200).containsEntry("maximumAllowedGapMillis", 500)
                    .containsEntry("helloCommands", sampler.samplesRecorded()).containsEntry("setupPingCommands", 1);
            List<?> readings = (List<?>) evidence.get("readings");
            assertThat(readings).hasSize(sampler.samplesRecorded());
            String firstProcess = String.valueOf(((Map<?, ?>) readings.getFirst()).get("processId"));
            assertThat(readings).allSatisfy(reading -> assertThat(String.valueOf(((Map<?, ?>) reading).get("processId"))).isEqualTo(firstProcess));
        }
    }
}
