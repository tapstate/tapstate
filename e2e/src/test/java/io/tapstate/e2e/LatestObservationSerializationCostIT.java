package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Default cost gate over an actual steady-state inline publication, with no wall-clock threshold. */
@RequiresDocker
class LatestObservationSerializationCostIT {
    @Test
    void oneRoutineInlinePublicationUsesOneCompletePayloadEncoding() throws Exception {
        try (var artifact = reactor()) {
            String mode = Boolean.getBoolean("tapstate.cost-gate.latest-extra-encode")
                    ? "store-latest-extra" : "store-latest";
            assertCost(BenchmarkJdiCostObserver.run(artifact, mode, BenchmarkJdiCostObserver.Options.NORMAL,
                    SharedMongo.replicaSetUrl("latest_observation_serialization_cost")));
        }
    }

    @Test
    void anUnusedSecondPayloadEncodingCannotHideBehindTheSameStoredObservation() throws Exception {
        try (var artifact = reactor()) {
            assertThatThrownBy(() -> assertCost(BenchmarkJdiCostObserver.run(artifact, "store-latest-extra",
                    BenchmarkJdiCostObserver.Options.NORMAL,
                    SharedMongo.replicaSetUrl("latest_observation_serialization_extra_cost"))))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("complete latest payload encodings");
        }
    }

    private static BenchmarkJdiCostObserver.Artifact reactor() throws Exception {
        String configured = System.getProperty("tapstate.e2e.boot-jar");
        if (configured == null || configured.isBlank()) { throw new AssertionError("latest encoder gate requires the reactor boot jar"); }
        return BenchmarkJdiCostObserver.Artifact.openReactor(Path.of(configured), PipelineBenchmarkLiveRunIT.harnessRoot());
    }

    private static void assertCost(BenchmarkJdiCostObserver.Summary result) {
        assertThat(result.require(BenchmarkJdiCostObserver.Unit.LATEST_OBSERVATION_BINARY_ENCODER_INVOCATION))
                .as("complete latest payload encodings").isEqualTo(new BenchmarkJdiCostObserver.Count(1, 1));
        assertThat(result.closedAndDrained()).isTrue();
        assertThat(result.methodEntryRequests()).isZero();
    }
}
