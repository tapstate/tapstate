package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Default first-event and nonempty closed-bucket serialization counts use their actual store paths. */
@RequiresDocker
class HistoryPublicationSerializationCostIT {
    @Test void unusedEventRepresentationConversionCannotHideBehindItsBinaryCount() throws Exception {
        try (var artifact = reactor()) {
            assertThatThrownBy(() -> assertCost("store-event", 1, BenchmarkJdiCostObserver.run(artifact,
                    "store-event-conversion", BenchmarkJdiCostObserver.Options.NORMAL,
                    SharedMongo.replicaSetUrl("event_representation_conversion_cost"))))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("event representation conversions");
        }
    }

    @Test void anOrdinaryEventHasOneDocumentBuildAndOneBinaryEncoding() throws Exception { run("store-event", 1); }
    @Test void aNonemptyRollupHasOneBuildAndItsExactNestedBinaryEncodings() throws Exception { run("store-rollup", 9); }

    @Test void extraEventEncodingIsRejectedDespiteAnIdenticalPersistedEvent() throws Exception { extra("store-event", 1); }
    @Test void extraRollupEncodingIsRejectedDespiteAnIdenticalPersistedBucket() throws Exception { extra("store-rollup", 9); }

    private static void run(String mode, int encodes) throws Exception {
        try (var artifact = reactor()) {
            String selected = Boolean.getBoolean("tapstate.cost-gate." + mode + "-extra-encode") ? mode + "-extra" : mode;
            assertCost(mode, encodes, BenchmarkJdiCostObserver.run(artifact, selected,
                    BenchmarkJdiCostObserver.Options.NORMAL, SharedMongo.replicaSetUrl("history_serialization_cost")));
        }
    }

    private static void extra(String mode, int encodes) throws Exception {
        try (var artifact = reactor()) {
            assertThatThrownBy(() -> assertCost(mode, encodes, BenchmarkJdiCostObserver.run(artifact, mode + "-extra",
                    BenchmarkJdiCostObserver.Options.NORMAL, SharedMongo.replicaSetUrl("history_serialization_extra_cost"))))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("publication binary encoder invocations");
        }
    }

    private static BenchmarkJdiCostObserver.Artifact reactor() throws Exception {
        String configured = System.getProperty("tapstate.e2e.boot-jar");
        if (configured == null || configured.isBlank()) { throw new AssertionError("history encoder gate requires the reactor boot jar"); }
        return BenchmarkJdiCostObserver.Artifact.openReactor(Path.of(configured), PipelineBenchmarkLiveRunIT.harnessRoot());
    }

    private static void assertCost(String mode, int encodes, BenchmarkJdiCostObserver.Summary result) {
        var unit = mode.equals("store-event") ? BenchmarkJdiCostObserver.Unit.EVENT_DOCUMENT_BUILD
                : BenchmarkJdiCostObserver.Unit.ROLLUP_DOCUMENT_BUILD;
        assertThat(result.require(unit)).as("one production history document build")
                .isEqualTo(new BenchmarkJdiCostObserver.Count(1, 1));
        assertThat(result.require(BenchmarkJdiCostObserver.Unit.BSON_BINARY_ENCODER_INVOCATION))
                .as("publication binary encoder invocations").isEqualTo(new BenchmarkJdiCostObserver.Count(encodes, encodes));
        if (mode.equals("store-rollup")) {
            assertThat(result.require(BenchmarkJdiCostObserver.Unit.BSON_REPRESENTATION_CONVERSION))
                    .as("rollup filter representation conversions").isEqualTo(new BenchmarkJdiCostObserver.Count(1, 1));
        } else {
            assertThat(result.require(BenchmarkJdiCostObserver.Unit.BSON_REPRESENTATION_CONVERSION))
                    .as("event representation conversions").isEqualTo(new BenchmarkJdiCostObserver.Count(0, 0));
        }
        assertThat(result.closedAndDrained()).isTrue();
        assertThat(result.methodEntryRequests()).isZero();
    }
}
