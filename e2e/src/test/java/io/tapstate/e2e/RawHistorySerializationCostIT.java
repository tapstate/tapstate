package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Default build-local cost gate over one actual raw history publication, never a wall-clock gate. */
@RequiresDocker
class RawHistorySerializationCostIT {
    @Test
    void anUnusedRepresentationConversionCannotHideBehindUnchangedBinaryCounts() throws Exception {
        try (var artifact = reactorArtifact()) {
            assertThatThrownBy(() -> assertCost(BenchmarkJdiCostObserver.run(artifact, "store-raw-conversion",
                    BenchmarkJdiCostObserver.Options.NORMAL, SharedMongo.replicaSetUrl("raw_history_conversion_cost"))))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("raw publication representation conversions");
        }
    }

    @Test
    void oneActualRawAppendBuildsAndEncodesExactlyItsKnownDocuments() throws Exception {
        try (var artifact = reactorArtifact()) {
            assertThat(artifact.reactorBuild).as("the separately validated build-local artifact policy").isTrue();
            String mode = Boolean.getBoolean("tapstate.cost-gate.raw-extra-encode") ? "store-raw-extra" : "store-raw";
            assertCost(BenchmarkJdiCostObserver.run(artifact, mode, BenchmarkJdiCostObserver.Options.NORMAL,
                    SharedMongo.replicaSetUrl("raw_history_serialization_cost")));
        }
    }

    @Test
    void aRealRedundantEncodingFailsTheSameCostAssertions() throws Exception {
        try (var artifact = reactorArtifact()) {
            assertThatThrownBy(() -> assertCost(BenchmarkJdiCostObserver.run(artifact, "store-raw-extra",
                    BenchmarkJdiCostObserver.Options.NORMAL,
                    SharedMongo.replicaSetUrl("raw_history_serialization_extra_cost"))))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("binary encoder invocations");
        }
    }

    private static BenchmarkJdiCostObserver.Artifact reactorArtifact() throws Exception {
        String configured = System.getProperty("tapstate.e2e.boot-jar");
        if (configured == null || configured.isBlank()) { throw new AssertionError("default raw encoder gate requires its reactor boot jar"); }
        return BenchmarkJdiCostObserver.Artifact.openReactor(Path.of(configured), PipelineBenchmarkLiveRunIT.harnessRoot());
    }

    private static void assertCost(BenchmarkJdiCostObserver.Summary result) {
        assertThat(result.require(BenchmarkJdiCostObserver.Unit.RATE_DOCUMENT_BUILD))
                .as("one production rate document build").isEqualTo(new BenchmarkJdiCostObserver.Count(1, 1));
        assertThat(result.require(BenchmarkJdiCostObserver.Unit.BSON_BINARY_ENCODER_INVOCATION))
                .as("raw publication binary encoder invocations").isEqualTo(new BenchmarkJdiCostObserver.Count(3, 3));
        assertThat(result.require(BenchmarkJdiCostObserver.Unit.BSON_REPRESENTATION_CONVERSION))
                .as("raw publication representation conversions").isEqualTo(new BenchmarkJdiCostObserver.Count(0, 0));
        assertThat(result.closedAndDrained()).isTrue();
        assertThat(result.methodEntryRequests()).isZero();
    }
}
