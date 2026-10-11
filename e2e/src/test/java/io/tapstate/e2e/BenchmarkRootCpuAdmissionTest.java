package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkRootCpuAdmissionTest {
    private static final String PROPERTY = RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY;
    private static final String LIBRARY = "/private/tmp/controlled-root-cpu-clock.dylib";

    @Test void the_absent_property_never_opens_a_provider_or_adds_child_arguments_or_failure_receipts() {
        try (var properties = new Properties()) {
            assertThat(RealBenchmarkForkDriver.rootCpuDiagnostics(null, null)).isFalse();
            assertThat(RealBenchmarkForkDriverIT.requireRootCpuAdmission("copy")).isFalse();
            assertThat(RealBenchmarkForkDriverIT.requireRootCpuRunServices("copy", () -> {
                throw new AssertionError("default run repeated its BeforeAll service checks");
            })).isFalse();
            var opened = new AtomicInteger();
            assertThat(RealBenchmarkForkDriver.openRootCpuDiagnostics(false, () -> {
                opened.incrementAndGet(); throw new AssertionError("unexpected CPU provider construction");
            })).isNull();
            RealBenchmarkForkDriver.rootCpuAfterAck(null, null);
            RealBenchmarkForkDriver.retainRootCpuRefusal(null, new AssertionError("unrelated failure"), receipt -> {
                throw new AssertionError("unexpected root CPU receipt");
            });
            assertThat(opened).hasValue(0);
            for (var calibration : RealBenchmarkForkDriver.CollectorCalibration.values()) {
                assertThat(RealBenchmarkForkDriverIT.collectorCalibrationArguments(calibration, LIBRARY)).containsExactly(
                        "-Dtapstate.benchmark.write-return=" + (calibration == RealBenchmarkForkDriver.CollectorCalibration.ON),
                        "-Dtapstate.benchmark.native-clock-library=" + LIBRARY);
            }
        }
    }

    @Test void explicit_false_invalid_or_unscoped_cpu_requests_refuse_before_services_or_artifacts() {
        try (var properties = new Properties()) {
            for (String value : List.of("false", "TRUE", "", "invalid", "true")) {
                properties.clear(); System.setProperty(PROPERTY, value);
                assertThatThrownBy(RealBenchmarkForkDriverIT::requireServices)
                        .isInstanceOf(AssertionError.class).hasMessageContaining("root CPU diagnostics");
                assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(
                        BenchmarkWorkloadDefinitions.steadyPilot("stateless"), PipelineBenchmarkComparison.Arm.B, 1, null))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("root CPU diagnostics");
            }
        }
    }

    @Test void the_real_entry_allows_only_both_original_isolated_native_collector_modes() {
        try (var properties = new Properties()) {
            for (String calibration : List.of("ON", "OFF")) {
                properties.valid(calibration);
                assertThat(RealBenchmarkForkDriverIT.requireRootCpuAdmission("stateless")).isTrue();
                assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(
                        BenchmarkWorkloadDefinitions.steadyPilot("stateless"), PipelineBenchmarkComparison.Arm.B, 1, null))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("application JAR");
                assertThatThrownBy(() -> RealBenchmarkForkDriverIT.requireRootCpuAdmission("copy"))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("collector calibration");
                assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(
                        BenchmarkWorkloadDefinitions.steadyPilot("stateless"), PipelineBenchmarkComparison.Arm.A, 1, null))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("collector calibration");
                assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(
                        BenchmarkWorkloadDefinitions.byId("stateless"), PipelineBenchmarkComparison.Arm.B, 1, null))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("collector calibration");
            }
            for (String missing : List.of(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY,
                    RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, "tapstate.e2e.benchmark-smoke.fork-output")) {
                properties.valid("ON"); System.clearProperty(missing);
                assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(
                        BenchmarkWorkloadDefinitions.steadyPilot("stateless"), PipelineBenchmarkComparison.Arm.B, 1, null))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("collector calibration");
            }
            for (String conflict : List.of(RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY,
                    RealBenchmarkForkDriver.WRITE_RETURN_COST_STAGES_PROPERTY, RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY,
                    RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY,
                    "tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics", "tapstate.e2e.benchmark-smoke.paced-calibration")) {
                properties.valid("ON"); System.setProperty(conflict, "true");
                assertThatThrownBy(() -> RealBenchmarkForkDriverIT.requireRootCpuAdmission("stateless"))
                        .isInstanceOf(AssertionError.class);
                assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(
                        BenchmarkWorkloadDefinitions.steadyPilot("stateless"), PipelineBenchmarkComparison.Arm.B, 1, null))
                        .isInstanceOf(AssertionError.class);
            }
            properties.valid("ON"); System.setProperty("tapstate.e2e.benchmark-smoke.capture-mode", "COST");
            assertThatThrownBy(() -> RealBenchmarkForkDriverIT.requireRootCpuAdmission("stateless"))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("collector calibration");
        }
    }

    @Test void formal_entry_refuses_every_explicit_cpu_property_before_configuration() {
        try (var properties = new Properties()) {
            for (String value : List.of("true", "false", "invalid", "")) {
                properties.clear(); System.setProperty(PROPERTY, value);
                assertThatThrownBy(() -> new PipelineBenchmarkLiveRunIT().interleavedRealForksWriteEvidenceAndEnforceTheSelectedGate())
                        .isInstanceOf(AssertionError.class).hasMessageContaining("root CPU diagnostics");
            }
        }
    }

    @Test void valid_cpu_requests_for_foreign_workloads_never_invoke_services_before_refusal() {
        try (var properties = new Properties()) {
            for (String calibration : List.of("ON", "OFF")) {
                properties.valid(calibration); RealBenchmarkForkDriverIT.requireServices();
                var services = new AtomicInteger();
                for (String workload : List.of("copy", "stateful")) {
                    assertThatThrownBy(() -> RealBenchmarkForkDriverIT.requireRootCpuRunServices(workload, services::incrementAndGet))
                            .isInstanceOf(AssertionError.class).hasMessageContaining("collector calibration");
                    assertThat(services).hasValue(0);
                }
                assertThat(RealBenchmarkForkDriverIT.requireRootCpuRunServices("stateless", services::incrementAndGet)).isTrue();
                assertThat(services).hasValue(1);
            }
        }
    }

    @Test void common_and_final_hooks_cover_all_four_tickets_before_the_final_cpu_read() {
        var fixture = new CpuFixture(); var envelope = fixture.envelope();
        envelope.read(BenchmarkRootCpuEnvelope.Cutoff.BEFORE_COLLECTION);
        var common = envelope.begin("MEASURED_COMMON_COLLECTION");
        RealBenchmarkForkDriver.rootCpuAfterAck(envelope, common);
        assertThat(fixture.cpuReads).hasValue(2);
        var capture = envelope.begin("CAPTURE_TAIL"); envelope.complete(capture);
        var sampler = envelope.begin("SAMPLER_NATIVE_TAIL"); envelope.complete(sampler);
        var oracle = envelope.begin("ORACLE_ASSOCIATION_TAIL");
        Map<String, Object> receipt = RealBenchmarkForkDriver.finishRootCpuDiagnostics(envelope, oracle);
        assertThat(fixture.cpuReads).hasValue(3);
        assertThat(receipt).containsEntry("state", "RECORDED_DIAGNOSTIC");
        var readings = rows(receipt, "readings"); assertThat(readings).hasSize(3);
        assertThat(readings.get(1).get("cutoff")).isEqualTo("AFTER_ACK_COMMON_CHECKPOINTS");
        long finalStarted = (long) readings.get(2).get("startedAtNanos");
        assertThat(rows(receipt, "operations")).hasSize(4).allSatisfy(operation -> {
            assertThat(operation).containsEntry("state", "COMPLETED");
            assertThat((long) operation.get("completedAtNanos")).isLessThanOrEqualTo(finalStarted);
        });
        assertThat(((Map<?, ?>) receipt.get("accountingErrorAllowance")).get("state")).isEqualTo("UNKNOWN");
        assertThat(((Map<?, ?>) receipt.get("collectionCpuUpperBound")).get("state")).isEqualTo("UNKNOWN");
        assertThatThrownBy(receipt::clear).isInstanceOf(UnsupportedOperationException.class);
        for (String flag : List.of("accountingErrorBoundQualified", "wholeMethodCostQualified", "collectionCostUpperBoundQualified",
                "samplingCostQualified", "causalOverheadQualified", "costAcceptanceEligible", "performanceAcceptanceEligible", "formalPerformance")) {
            assertThat(receipt.get(flag)).isEqualTo(false);
        }
    }

    @Test void an_unfinished_tail_refuses_without_a_final_read_and_retains_the_original_pending_receipt() {
        var fixture = new CpuFixture(); var envelope = fixture.envelope();
        envelope.read(BenchmarkRootCpuEnvelope.Cutoff.BEFORE_COLLECTION);
        RealBenchmarkForkDriver.rootCpuAfterAck(envelope, envelope.begin("MEASURED_COMMON_COLLECTION"));
        envelope.complete(envelope.begin("CAPTURE_TAIL"));
        var sampler = envelope.begin("SAMPLER_NATIVE_TAIL");
        var oracle = envelope.begin("ORACLE_ASSOCIATION_TAIL");
        var primary = new TimeoutException("controlled unfinished sampler tail"); fixture.waitFailure = primary;
        var refusal = org.assertj.core.api.Assertions.catchThrowableOfType(
                () -> RealBenchmarkForkDriver.finishRootCpuDiagnostics(envelope, oracle), BenchmarkRootCpuEnvelope.Refusal.class);
        assertThat(refusal.getCause()).isSameAs(primary); assertThat(fixture.cpuReads).hasValue(2);
        var recorded = new AtomicReference<Map<String, Object>>();
        RealBenchmarkForkDriver.retainRootCpuRefusal(envelope, refusal, recorded::set);
        assertThat(recorded.get()).containsEntry("state", "UNKNOWN").containsEntry("rootCpuRefusal", refusal.retainedEvidence());
        var initial = map(recorded.get(), "rootCpuRefusal");
        assertThat(map(initial, "pendingRead")).containsKey("cutoff").doesNotContainKeys("startedAtNanos", "completedAtNanos");
        assertThat(rows(initial, "readings")).hasSize(2);
        envelope.complete(sampler);
        assertThat(envelope.evidence()).containsEntry("state", "UNKNOWN");
        assertThat(rows(initial, "operations")).anySatisfy(operation -> assertThat(operation).containsEntry("state", "PENDING"));
        var recording = new AssertionError("controlled receipt recording failure");
        RealBenchmarkForkDriver.retainRootCpuRefusal(envelope, refusal, receipt -> { throw recording; });
        assertThat(refusal.getCause()).isSameAs(primary); assertThat(refusal.getSuppressed()).containsExactly(recording);
        assertThat(fixture.cpuReads).hasValue(2);
    }

    private static final class CpuFixture {
        final AtomicInteger cpuReads = new AtomicInteger();
        final AtomicLong clock = new AtomicLong(1000);
        volatile TimeoutException waitFailure;
        BenchmarkRootCpuEnvelope envelope() {
            var provider = new BenchmarkRootCpuEnvelope.ProviderIdentity("test.RootCpuProvider", "getProcessCpuTime",
                    "libmanagement_ext.dylib", "a".repeat(64));
            var expected = new BenchmarkRootCpuEnvelope.Identity(17, 1000, true, provider);
            return BenchmarkRootCpuEnvelope.from(expected, new BenchmarkRootCpuEnvelope.Provider() {
                public BenchmarkRootCpuEnvelope.Identity identity() { return expected; }
                public long processCpuTime() { return cpuReads.incrementAndGet() * 100L; }
            }, clock::incrementAndGet, Set.of("MEASURED_COMMON_COLLECTION"), Set.of(
                    "CAPTURE_TAIL", "SAMPLER_NATIVE_TAIL", "ORACLE_ASSOCIATION_TAIL"), (completion, timeout, unit) -> {
                        assertThat(unit).isEqualTo(TimeUnit.NANOSECONDS);
                        assertThat(timeout).isPositive().isLessThanOrEqualTo(BenchmarkRootCpuEnvelope.WAIT_LIMIT.toNanos());
                        if (waitFailure != null) { throw waitFailure; }
                        completion.get(timeout, unit);
                    });
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> rows(Map<String, Object> evidence, String key) {
        return (List<Map<String, Object>>) evidence.get(key);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Map<String, Object> evidence, String key) {
        return (Map<String, Object>) evidence.get(key);
    }

    private static final class Properties implements AutoCloseable {
        private final Map<String, String> previous = new LinkedHashMap<>();
        Properties() {
            for (String key : List.of(PROPERTY, RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY,
                    RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY,
                    RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY, RealBenchmarkForkDriver.WRITE_RETURN_COST_STAGES_PROPERTY,
                    RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY, RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY,
                    "tapstate.e2e.benchmark-smoke.arm", "tapstate.e2e.benchmark-smoke.jar", "tapstate.e2e.benchmark-smoke.steady-pilot",
                    "tapstate.e2e.benchmark-smoke.capture-mode", "tapstate.e2e.benchmark-smoke.fork-output",
                    "tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics", "tapstate.e2e.benchmark.compilation-diagnostics",
                    "tapstate.e2e.benchmark.thread-point-diagnostics", "tapstate.e2e.benchmark.load-diagnostics",
                    BenchmarkDualGcDiagnostics.ENABLED_PROPERTY, BenchmarkWitnessReadGate.PROPERTY,
                    BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY,
                    BenchmarkMongoDeliveryObserver.NATIVE_OPERATION_WALL_EVIDENCE_PROPERTY,
                    "tapstate.e2e.benchmark-smoke.paced-calibration", "tapstate.e2e.benchmark-smoke.cdc-settling-calibration",
                    "tapstate.e2e.benchmark-smoke.full-cdc-settling-calibration")) {
                previous.put(key, System.getProperty(key)); System.clearProperty(key);
            }
        }
        void clear() { previous.keySet().forEach(System::clearProperty); }
        void valid(String calibration) {
            clear(); System.setProperty(PROPERTY, "true");
            System.setProperty(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY, calibration);
            System.setProperty(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, "true");
            System.setProperty(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY, LIBRARY);
            System.setProperty("tapstate.e2e.benchmark-smoke.arm", "B");
            System.setProperty("tapstate.e2e.benchmark-smoke.steady-pilot", "true");
            System.setProperty("tapstate.e2e.benchmark-smoke.capture-mode", "PLAIN");
            System.setProperty("tapstate.e2e.benchmark-smoke.fork-output", "/private/tmp/root-cpu-admission-fork.json");
        }
        public void close() {
            previous.forEach((key, value) -> { if (value == null) { System.clearProperty(key); } else { System.setProperty(key, value); } });
        }
    }
}
