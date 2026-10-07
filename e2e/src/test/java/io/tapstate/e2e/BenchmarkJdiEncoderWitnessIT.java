package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.EnumMap;
import java.util.Map;

import static io.tapstate.e2e.BenchmarkJdiCostObserver.Arm;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.Count;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.Namespace;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.Operation;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.Options;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.Unit;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.Unavailable;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.WireKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Isolated exactness witnesses; debugger windows are never performance samples. */
class BenchmarkJdiEncoderWitnessIT {

    private static final String REFERENCE_PROPERTY = "tapstate.e2e.jdi-cost.reference-jar";
    private static final String OBSERVABILITY_PROPERTY = "tapstate.e2e.jdi-cost.observability-jar";
    private static final Map<Arm, BenchmarkJdiCostObserver.Artifact> ARTIFACTS = new EnumMap<>(Arm.class);

    @BeforeAll
    static void immutableArtifacts() throws Exception {
        String reference = System.getProperty(REFERENCE_PROPERTY);
        String observability = System.getProperty(OBSERVABILITY_PROPERTY);
        assumeTrue(reference != null || observability != null,
                "The isolated JDI witness requires both explicitly supplied immutable artifacts.");
        assertThat(reference).as("the reference artifact input").isNotBlank();
        assertThat(observability).as("the observability artifact input").isNotBlank();
        ARTIFACTS.put(Arm.REFERENCE,
                BenchmarkJdiCostObserver.Artifact.open(Path.of(reference), Arm.REFERENCE,
                        BenchmarkJdiCostObserver.selectedArtifactSet()));
        ARTIFACTS.put(Arm.OBSERVABILITY,
                BenchmarkJdiCostObserver.Artifact.open(Path.of(observability), Arm.OBSERVABILITY,
                        BenchmarkJdiCostObserver.selectedArtifactSet()));
    }

    @AfterAll
    static void discardOnlyOwnedExtractionDirectories() throws Exception {
        for (var artifact : ARTIFACTS.values()) {
            artifact.close();
        }
        ARTIFACTS.clear();
    }

    @Test
    void exactEntryAndReturnBreakpointsCountBothImmutableArtifacts() throws Exception {
        var diagnostic = BenchmarkJdiCostObserver.targetFailure(
                "TARGET_FAILED\tORIGINS\tjava.lang.NoClassDefFoundError\tio.tapstate.spi.store.ObservationStore");
        assertThat(diagnostic.diagnostic().phase()).isEqualTo(BenchmarkJdiCostObserver.TargetPhase.ORIGINS);
        assertThat(diagnostic.diagnostic().exceptionClass()).isEqualTo("java.lang.NoClassDefFoundError");
        assertThat(diagnostic.diagnostic().linkageSymbol()).isEqualTo("io.tapstate.spi.store.ObservationStore");
        for (String rejected : new String[]{
                "TARGET_FAILED\tUNKNOWN\tjava.lang.NoClassDefFoundError\tio.tapstate.spi.store.ObservationStore",
                "TARGET_FAILED\tORIGINS\tjava.lang.NoClassDefFoundError\tmongodb://user:secret@localhost",
                "TARGET_FAILED\tWIRE_SETUP\tcom.mongodb.MongoException\tsecret.message"}) {
            assertThatThrownBy(() -> BenchmarkJdiCostObserver.targetFailure(rejected))
                    .isInstanceOf(AssertionError.class).hasMessageNotContaining("secret");
        }
        for (String reported : new String[]{"127.0.0.1:49152", "localhost:49152", "[::1]:49152"}) {
            assertThat(BenchmarkJdiCostObserver.numericLoopbackDialAddress("127.0.0.1", reported, "49152"))
                    .isEqualTo("127.0.0.1:49152");
        }
        for (String reported : new String[]{"0.0.0.0:49152", "192.0.2.1:49152", "localhost:0",
                "localhost:65536", "localhost:49153", "localhost:49152/extra"}) {
            assertThatThrownBy(() -> BenchmarkJdiCostObserver.numericLoopbackDialAddress(
                    "127.0.0.1", reported, "49152"))
                    .isInstanceOf(AssertionError.class);
        }
        assertThatThrownBy(() -> BenchmarkJdiCostObserver.numericLoopbackDialAddress(
                "0.0.0.0", "localhost:49152", "49152"))
                .isInstanceOf(AssertionError.class).hasMessageContaining("required numeric loopback bind");
        for (Arm arm : Arm.values()) {
            var summary = BenchmarkJdiCostObserver.run(ARTIFACTS.get(arm), "normal", Options.NORMAL, null);
            assertThat(summary.artifactSha256()).isEqualTo(ARTIFACTS.get(arm).artifactSha256);
            assertThat(summary.require(Unit.OBSERVATION_DOCUMENT_BUILD)).isEqualTo(new Count(3, 3));
            assertThat(summary.require(Unit.RATE_DOCUMENT_BUILD)).isEqualTo(new Count(2, 2));
            assertThat(summary.require(Unit.BSON_BINARY_ENCODER_INVOCATION)).isEqualTo(new Count(5, 5));
            assertThat(summary.breakpointEvents()).isEqualTo(20);
            assertThat(summary.methodEntryRequests()).isZero();
            assertThat(summary.breakpointRequests()).isGreaterThanOrEqualTo(6);
            assertThat(summary.observerHandlingNanos()).isPositive();
            assertThat(summary.closedAndDrained()).isTrue();
            assertThat(summary.wireCommands()).isEmpty();
            // Calls to the same class's unrelated getEncoderClass and bridge methods add no events.
            assertThat(summary.counts()).hasSize(3);
        }
    }

    @Test
    void oneExtraActualBinaryEncodingChangesTheCountByOneOnBothArms() throws Exception {
        for (Arm arm : Arm.values()) {
            var summary = BenchmarkJdiCostObserver.run(ARTIFACTS.get(arm), "extra", Options.NORMAL, null);
            assertThat(summary.require(Unit.BSON_BINARY_ENCODER_INVOCATION)).isEqualTo(new Count(6, 6));
            assertThat(summary.require(Unit.OBSERVATION_DOCUMENT_BUILD)).isEqualTo(new Count(3, 3));
            assertThat(summary.breakpointEvents()).isEqualTo(22);
        }
    }

    @Test
    void latestBinaryEncodingHasAnExplicitReferenceAbsence() throws Exception {
        assertThat(ARTIFACTS.get(Arm.REFERENCE).latestAvailable()).isFalse();
        var reference = BenchmarkJdiCostObserver.run(
                ARTIFACTS.get(Arm.REFERENCE), "normal", Options.NORMAL, null);
        assertThat(reference.unavailable()).contains(Unavailable.LATEST_OBSERVATION_BINARY_ENCODER);
        assertThat(reference.counts()).doesNotContainKey(Unit.LATEST_OBSERVATION_BINARY_ENCODER_INVOCATION);
        assertThatThrownBy(() -> reference.require(Unit.LATEST_OBSERVATION_BINARY_ENCODER_INVOCATION))
                .isInstanceOf(AssertionError.class).hasMessageContaining("not measured");
        var candidate = BenchmarkJdiCostObserver.run(
                ARTIFACTS.get(Arm.OBSERVABILITY), "latest", Options.NORMAL, null);
        assertThat(candidate.require(Unit.LATEST_OBSERVATION_BINARY_ENCODER_INVOCATION))
                .isEqualTo(new Count(3, 3));
        assertThat(candidate.unavailable()).doesNotContain(Unavailable.LATEST_OBSERVATION_BINARY_ENCODER);
        assertThat(candidate.breakpointEvents()).isEqualTo(6);
        assertThat(candidate.closedAndDrained()).isTrue();
    }

    @Test
    void aNonemptyChunkUsesOneProtectedLatestPayloadEncoding() throws Exception {
        var candidate = BenchmarkJdiCostObserver.run(
                ARTIFACTS.get(Arm.OBSERVABILITY), "latest-chunk", Options.NORMAL, null);
        assertThat(candidate.require(Unit.LATEST_OBSERVATION_BINARY_ENCODER_INVOCATION))
                .isEqualTo(new Count(1, 1));
        assertThat(candidate.breakpointEvents()).isEqualTo(2);
        assertThat(candidate.closedAndDrained()).isTrue();
        assertThatThrownBy(() -> BenchmarkJdiCostObserver.run(
                ARTIFACTS.get(Arm.REFERENCE), "latest-chunk", Options.NORMAL, null))
                .isInstanceOf(AssertionError.class).hasMessageContaining("unavailable");
    }

    @Test
    void losingAnEntryEventInvalidatesTheWholeWindow() {
        assertThatThrownBy(() -> BenchmarkJdiCostObserver.run(ARTIFACTS.get(Arm.REFERENCE), "normal",
                new Options(true, 10_000), null))
                .isInstanceOf(AssertionError.class).hasMessageContaining("no matching exact entry event");
    }

    @Test
    void eventBudgetExhaustionInvalidatesTheWholeWindow() {
        assertThatThrownBy(() -> BenchmarkJdiCostObserver.run(ARTIFACTS.get(Arm.OBSERVABILITY), "normal",
                new Options(false, 1), null))
                .isInstanceOf(AssertionError.class).hasMessageContaining("event budget");
    }

    @Test
    void aThrownEncodingCannotBeCountedAsCompletedOrReplacedWithZero() {
        assertThatThrownBy(() -> BenchmarkJdiCostObserver.run(
                ARTIFACTS.get(Arm.OBSERVABILITY), "exception", Options.NORMAL, null))
                .hasMessageContaining("unmatched entry");
    }

    @Test
    void aSecondLoaderForAnExactClassInvalidatesTheWindow() {
        assertThatThrownBy(() -> BenchmarkJdiCostObserver.run(
                ARTIFACTS.get(Arm.REFERENCE), "duplicate", Options.NORMAL, null))
                .isInstanceOf(AssertionError.class).hasMessageContaining("duplicate or unexpected loader");
    }

    @Test
    void aTargetDisconnectBeforeTheEndBarrierInvalidatesTheWindow() {
        assertThatThrownBy(() -> BenchmarkJdiCostObserver.run(
                ARTIFACTS.get(Arm.REFERENCE), "disconnect", Options.NORMAL, null))
                .isInstanceOf(AssertionError.class).hasMessageContaining("before");
    }

    @Test
    void aTerminalCollectorFailureJustBeforePublicationKeepsItsOriginalDiagnostic() {
        assertThatThrownBy(() -> BenchmarkJdiCostObserver.run(ARTIFACTS.get(Arm.REFERENCE), "normal",
                new Options(false, 10_000, true), null))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("JDI collection failed before a complete drained window");
    }

    @Test
    void outputAfterTheStopBarrierInvalidatesTheCompletedWindow() {
        assertThatThrownBy(() -> BenchmarkJdiCostObserver.run(
                ARTIFACTS.get(Arm.REFERENCE), "post-done", Options.NORMAL, null))
                .isInstanceOf(AssertionError.class).hasMessageContaining("post-DONE");
    }

    @Test
    void anUnterminatedOversizedStderrLineCannotBecomeAValidSummary() {
        assertThatThrownBy(() -> BenchmarkJdiCostObserver.run(
                ARTIFACTS.get(Arm.REFERENCE), "stderr-oversized", Options.NORMAL, null))
                .isInstanceOf(AssertionError.class).hasMessageContaining("bounded target output budget");
    }

    @Test
    void aBulkInsertIsOneCompletedWireSendWithSeparateControlCommands() throws Exception {
        String uri = wireUri();
        for (Arm arm : Arm.values()) {
            var summary = BenchmarkJdiCostObserver.run(ARTIFACTS.get(arm), "wire", Options.NORMAL, uri);
            assertThat(summary.wireCommands().get(new WireKey(Namespace.OBSERVATION, Operation.INSERT)))
                    .isEqualTo(new Count(1, 1));
            assertThat(summary.wireCommands().get(new WireKey(Namespace.OBSERVATION, Operation.AGGREGATE)))
                    .isEqualTo(new Count(1, 1));
            Count control = summary.wireCommands().get(new WireKey(Namespace.CONTROL, Operation.CONTROL));
            assertThat(control).isNotNull();
            assertThat(control.normalReturns()).isGreaterThanOrEqualTo(1);
            Count sends = summary.require(Unit.SYNC_COMMAND_SEND);
            assertThat(sends.entries()).isEqualTo(sends.normalReturns());
            assertThat(sends.entries()).isEqualTo(summary.wireCommands().values().stream()
                    .mapToLong(Count::entries).sum());
            assertThat(summary.unavailable()).contains(Unavailable.ASYNC_WRITE_COMPLETION,
                    Unavailable.WIRE_DOCUMENT_COUNT, Unavailable.WIRE_BYTE_COUNT, Unavailable.BACKGROUND_BATCH_IO);
            assertThat(summary.closedAndDrained()).isTrue();
        }
    }

    @Test
    void anUnmappedWireNamespaceCannotDisappearAsZero() throws Exception {
        String uri = wireUri();
        assertThatThrownBy(() -> BenchmarkJdiCostObserver.run(
                ARTIFACTS.get(Arm.OBSERVABILITY), "wire-unmapped", Options.NORMAL, uri))
                .isInstanceOf(AssertionError.class).hasMessageContaining("unmapped namespace");
    }

    @Test
    void lifecycleCheckpointIoKeepsItsOwnExactNamespaceOnBothArtifacts() throws Exception {
        String uri = wireUri();
        for (Arm arm : Arm.values()) {
            var summary = BenchmarkJdiCostObserver.run(ARTIFACTS.get(arm), "wire-pipeline-state", Options.NORMAL, uri);
            assertThat(summary.wireCommands().get(new WireKey(Namespace.PIPELINE_STATE, Operation.INSERT)))
                    .isEqualTo(new Count(1, 1));
            assertThat(summary.wireCommands()).doesNotContainKey(new WireKey(Namespace.OBSERVATION, Operation.INSERT));
            assertThat(summary.closedAndDrained()).isTrue();
        }
    }

    private static String wireUri() {
        assumeTrue(Boolean.getBoolean("tapstate.e2e.jdi-cost.wire"),
                "Mongo-backed wire witnesses require an explicit opt-in.");
        DockerGate.require();
        return SharedMongo.replicaSetUrl("jdi_cost_witness");
    }
}
