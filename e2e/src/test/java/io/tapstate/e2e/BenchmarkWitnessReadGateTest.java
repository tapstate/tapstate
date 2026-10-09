package io.tapstate.e2e;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkWitnessReadGateTest {
    @Test
    void aReaderCannotConsumeBeforeAckAndCloseUnblocksItWithoutPermission() throws Exception {
        var gate = new BenchmarkWitnessReadGate(true);
        var entered = new CountDownLatch(1);
        var result = new CompletableFuture<Boolean>();
        Thread reader = Thread.ofVirtual().start(() -> {
            entered.countDown();
            try { result.complete(gate.awaitFirstRead()); }
            catch (Throwable failure) { result.completeExceptionally(failure); }
        });
        try {
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> result.get(40, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            var before = gate.evidence();
            gate.close();
            assertThat(result.get(1, TimeUnit.SECONDS)).isFalse();
            assertThat(reader.join(java.time.Duration.ofSeconds(1))).isTrue();
            assertThat(before).containsEntry("closed", false).containsEntry("firstTryNextCallStartedAtNanos", null);
            assertThat(gate.evidence()).containsEntry("closed", true).containsEntry("fullFirstPhaseDrainCompleted", false);
        } finally { gate.close(); reader.join(java.time.Duration.ofSeconds(1)); }
    }

    @Test
    void invalidAckOrEarlyReadCannotBecomeAQualifiedDeferredReceipt() throws Exception {
        var gate = new BenchmarkWitnessReadGate(true);
        assertThatThrownBy(() -> gate.releaseAfterOwnAck(Long.MIN_VALUE)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> gate.releaseAfterOwnAck(Long.MAX_VALUE)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> gate.beforeFirstTryNext(System.nanoTime())).isInstanceOf(AssertionError.class);
        long ack = System.nanoTime();
        gate.releaseAfterOwnAck(ack);
        assertThat(gate.awaitFirstRead()).isTrue();
        long first = System.nanoTime();
        assertThat(gate.beforeFirstTryNext(first)).isTrue();
        assertThatThrownBy(() -> gate.completedPhase("cdc-update", 2, 1)).isInstanceOf(AssertionError.class);
        gate.completedPhase("cdc-update", 2, 2);
        assertThat(gate.evidence()).containsEntry("ownAckAtNanos", ack)
                .containsEntry("firstTryNextCallStartedAtNanos", first)
                .containsEntry("fullFirstPhaseDrainCompleted", true)
                .containsEntry("localDeliveryLatencyPerformanceEligible", false);
    }

    @Test
    void formalEntryAndAnInvalidSmokeRefuseBeforeAnyFilesOrServices() {
        String old = System.setProperty(BenchmarkWitnessReadGate.PROPERTY, "true");
        Map<String, String> priorFormal = new java.util.LinkedHashMap<>();
        for (String key : List.of("baseline-jar", "candidate-jar", "output", "gate", "target", "primary")) {
            String property = "tapstate.e2e.benchmark." + key;
            priorFormal.put(property, System.getProperty(property));
            System.clearProperty(property);
        }
        String oldArm = System.setProperty("tapstate.e2e.benchmark-smoke.arm", "A");
        String oldSmokeJar = System.setProperty("tapstate.e2e.benchmark-smoke.jar", "unused.jar");
        String oldOutput = System.getProperty("tapstate.e2e.benchmark-smoke.fork-output");
        System.clearProperty("tapstate.e2e.benchmark-smoke.fork-output");
        try {
            assertThatThrownBy(() -> new PipelineBenchmarkLiveRunIT()
                    .interleavedRealForksWriteEvidenceAndEnforceTheSelectedGate())
                    .isInstanceOf(AssertionError.class).hasMessageContaining("deferred target witness");
            assertThatThrownBy(() -> new RealBenchmarkForkDriverIT()
                    .copyForkUsesRealTargetChangesAndItsOwnTerminalPosition())
                    .isInstanceOf(AssertionError.class).hasMessageContaining("unchanged plain original copy B");
        } finally {
            restore(BenchmarkWitnessReadGate.PROPERTY, old);
            priorFormal.forEach(BenchmarkWitnessReadGateTest::restore);
            restore("tapstate.e2e.benchmark-smoke.arm", oldArm);
            restore("tapstate.e2e.benchmark-smoke.jar", oldSmokeJar);
            restore("tapstate.e2e.benchmark-smoke.fork-output", oldOutput);
        }
    }

    @Test
    void aRetainedDeferredPhaseCannotBecomeFormalAfterThePropertyIsCleared() {
        var resources = new BenchmarkResourceSampler.Summary(1, 0, 1, 1, 2);
        var phase = new RealBenchmarkForkDriver.MeasuredPhase("cdc-update", 48000, 1, 2, 3, 96000, 96000, 96000,
                new BenchmarkForkEnvironment.ClockAnchor(Instant.EPOCH, 1, 2), List.of(), resources,
                Optional.empty(), Optional.empty(), true, Optional.empty(), Map.of("targetWitnessDeferred", true));
        var evidence = new RealBenchmarkForkDriver.Evidence("copy-B-1", BenchmarkWorkloadDefinitions.steadyPilot("copy"),
                PipelineBenchmarkComparison.Arm.B, Path.of("unused.jar"), List.of(phase), resources,
                new BenchmarkMongoCommandSampler.Summary(Map.of(), Map.of(), 0), Map.of(), Map.of(), "unused", 0,
                List.of(), Optional.empty());
        assertThatThrownBy(evidence::requireSteadyStateWindow).isInstanceOf(AssertionError.class)
                .hasMessageContaining("deferred target witness");
    }

    private static void restore(String key, String value) {
        if (value == null) { System.clearProperty(key); }
        else { System.setProperty(key, value); }
    }
}
