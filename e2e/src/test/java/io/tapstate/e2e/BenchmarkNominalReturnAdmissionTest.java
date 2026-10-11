package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkNominalReturnAdmissionTest {
    private static final String PROPERTY = RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY;
    private static final String LIBRARY = "/private/tmp/controlled-nominal-return-clock.dylib";

    @Test void an_absent_property_keeps_integer_dispatch_and_never_calls_the_nominal_factory() throws Exception {
        try (var properties = new Properties()) {
            assertThat(RealBenchmarkForkDriver.nominalReturnDiagnostics(null, false, null)).isFalse();
            assertThat(RealBenchmarkForkDriverIT.requireNominalReturnAdmission("copy")).isFalse();
            assertThat(RealBenchmarkForkDriverIT.requireRootCpuRunServices("copy", () -> {
                throw new AssertionError("default run repeated its service checks");
            })).isFalse();
            var fixture = fixture(); var integers = new AtomicInteger(); var nominal = new AtomicInteger();
            var selected = RealBenchmarkForkDriver.selectNativeReturnClock(Boolean.getBoolean(PROPERTY), () -> {
                integers.incrementAndGet(); return BenchmarkWriteReturnPhaseEvidenceTest.integerClock(fixture.result());
            }, () -> {
                nominal.incrementAndGet(); throw new AssertionError("default path acquired nominal clock facts");
            });
            assertThat(selected).isInstanceOf(BenchmarkNativeReturnClock.class);
            var actual = record(fixture, fixture.result(), selected);
            assertThat(actual).isEqualTo(BenchmarkWriteReturnPhaseEvidence.record(fixture.workload(), fixture.phase(),
                    fixture.batches(), fixture.result(), (BenchmarkNativeReturnClock) selected));
            assertThat(actual).containsEntry("state", "CONDITIONAL_NATIVE_COUNTER_RETURN_BOUNDS")
                    .doesNotContainKey("returnTimeCoordinate");
            assertThat(integers).hasValue(1); assertThat(nominal).hasValue(0);
            assertThat(record(fixture, fixture.result(), null)).isEqualTo(BenchmarkWriteReturnPhaseEvidence.record(
                    fixture.workload(), fixture.phase(), fixture.batches(), fixture.result()));
        }
    }

    @Test void false_invalid_or_missing_native_prerequisites_refuse_before_services_and_artifacts() {
        try (var properties = new Properties()) {
            for (String value : List.of("false", "TRUE", "invalid", "", "true")) {
                properties.clear(); System.setProperty(PROPERTY, value);
                assertThatThrownBy(RealBenchmarkForkDriverIT::requireServices)
                        .isInstanceOf(AssertionError.class).hasMessageContaining("nominal return diagnostics");
                assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(
                        BenchmarkWorkloadDefinitions.steadyPilot("stateless"), PipelineBenchmarkComparison.Arm.B, 1, null))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("nominal return diagnostics");
            }
            properties.nativeDomain("B"); System.clearProperty(RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY);
            assertThatThrownBy(() -> RealBenchmarkForkDriverIT.requireNominalReturnAdmission("stateless"))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("nominal return diagnostics");
        }
    }

    @Test void actual_entry_admits_native_domain_a_and_b_or_enabled_collector_with_optional_root_cpu() {
        try (var properties = new Properties()) {
            for (String arm : List.of("A", "B")) {
                properties.nativeDomain(arm); assertAdmitted(PipelineBenchmarkComparison.Arm.valueOf(arm));
            }
            properties.collector("ON"); assertAdmitted(PipelineBenchmarkComparison.Arm.B);
            System.setProperty(RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY, "true");
            assertAdmitted(PipelineBenchmarkComparison.Arm.B);
            assertThat(RealBenchmarkForkDriverIT.returnArguments(false, false, LIBRARY)).containsExactly(
                    "-Dtapstate.benchmark.write-return=true", "-Dtapstate.benchmark.native-clock-library=" + LIBRARY);
        }
    }

    @Test void off_foreign_profiles_and_conflicts_refuse_before_the_actual_service_hook() {
        try (var properties = new Properties()) {
            properties.collector("OFF"); var services = new AtomicInteger();
            assertThatThrownBy(() -> RealBenchmarkForkDriverIT.requireRootCpuRunServices("stateless", services::incrementAndGet))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("nominal return diagnostics");
            assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(
                    BenchmarkWorkloadDefinitions.steadyPilot("stateless"), PipelineBenchmarkComparison.Arm.B, 1, null))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("nominal return diagnostics");
            assertThatThrownBy(() -> RealBenchmarkForkDriver.nominalReturnDiagnostics("true", true,
                    RealBenchmarkForkDriver.CollectorCalibration.OFF)).isInstanceOf(AssertionError.class);
            for (String workload : List.of("copy", "stateful")) {
                properties.nativeDomain("B"); RealBenchmarkForkDriverIT.requireServices();
                assertThatThrownBy(() -> RealBenchmarkForkDriverIT.requireRootCpuRunServices(workload, services::incrementAndGet))
                        .isInstanceOf(AssertionError.class);
            }
            assertThat(services).hasValue(0);
            properties.nativeDomain("B");
            assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(
                    BenchmarkWorkloadDefinitions.byId("stateless"), PipelineBenchmarkComparison.Arm.B, 1, null))
                    .isInstanceOf(AssertionError.class);
            for (String missing : List.of(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY,
                    RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, "tapstate.e2e.benchmark-smoke.fork-output")) {
                properties.nativeDomain("B"); System.clearProperty(missing);
                refuseBeforeServices(services);
            }
            properties.nativeDomain("B"); System.setProperty("tapstate.e2e.benchmark-smoke.capture-mode", "COST");
            refuseBeforeServices(services);
            for (String conflict : List.of(RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY,
                    RealBenchmarkForkDriver.WRITE_RETURN_COST_STAGES_PROPERTY, RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY,
                    "tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics", "tapstate.e2e.benchmark.load-diagnostics",
                    "tapstate.e2e.benchmark-smoke.paced-calibration")) {
                properties.nativeDomain("B"); System.setProperty(conflict, "true"); refuseBeforeServices(services);
            }
            properties.nativeDomain("B"); System.setProperty(RealBenchmarkForkDriver.WRITE_RETURN_COST_STAGES_PROPERTY, "false");
            refuseBeforeServices(services);
        }
    }

    @Test void every_explicit_nominal_property_refuses_formal_entry_before_configuration() {
        try (var properties = new Properties()) {
            for (String value : List.of("true", "false", "invalid", "")) {
                properties.clear(); System.setProperty(PROPERTY, value);
                assertThatThrownBy(() -> new PipelineBenchmarkLiveRunIT().interleavedRealForksWriteEvidenceAndEnforceTheSelectedGate())
                        .isInstanceOf(AssertionError.class).hasMessageContaining("nominal return");
            }
        }
    }

    @Test void selected_nominal_dispatch_encloses_the_same_complete_fixture_and_retains_its_bindings() throws Exception {
        var fixture = fixture(); var result = fixture.result();
        var integerClock = BenchmarkWriteReturnPhaseEvidenceTest.integerClock(result);
        var integers = new AtomicInteger(); var nominal = new AtomicInteger();
        var selected = RealBenchmarkForkDriver.selectNativeReturnClock(true, () -> {
            integers.incrementAndGet(); throw new AssertionError("nominal selection used an unrelated integer factory");
        }, () -> {
            nominal.incrementAndGet(); return BenchmarkWriteReturnPhaseEvidenceTest.nominalClock(result);
        });
        var old = record(fixture, result, integerClock); var current = record(fixture, result, selected);
        assertThat(integers).hasValue(0); assertThat(nominal).hasValue(1);
        assertThat(current).containsEntry("state", "CONDITIONAL_UNROUNDED_NOMINAL_RETURN_BOUNDS")
                .containsEntry("returnTimeCoordinate", "UNROUNDED_NOMINAL_COUNTER_ENCLOSURES")
                .containsEntry("fullRows", 96_000).containsEntry("fixedCohortRows", 48_000L);
        for (String field : List.of("pagesBase64", "sourceBatches", "clockSamples", "summary", "captureEpoch", "frameCount",
                "rawPageBytes", "pageCount", "callCount", "fullRows", "fixedCohortRows", "ownedRuntime", "endpoint", "rowTimeAssignment")) {
            assertThat(current.get(field)).isEqualTo(old.get(field));
        }
        for (String field : List.of("p99LatencyNanos", "completionSpanNanos")) {
            assertThat(bound(current, field, "lowerNanos")).isEqualTo(bound(old, field, "lowerNanos") - 1);
            assertThat(bound(current, field, "upperNanos")).isEqualTo(bound(old, field, "upperNanos") + 1);
        }
        var oldMapping = map(old, "nativeCounterDomainMapping"); var mapping = map(current, "nativeCounterDomainMapping");
        for (String field : List.of("snapshot", "counterUnit", "conditions", "reviewedJniSourceSha256", "bindingScope")) {
            assertThat(mapping.get(field)).isEqualTo(oldMapping.get(field));
        }
        assertThat(mapping).containsEntry("state", "CONDITIONAL_UNROUNDED_NOMINAL_COUNTER")
                .containsEntry("integerBindingState", oldMapping.get("state"));
        for (String flag : BenchmarkNativeClockEvidence.FLAGS) {
            assertThat(current.get(flag)).isEqualTo(false); assertThat(mapping.get(flag)).isEqualTo(false);
        }
        var associations = BenchmarkWriteReturnExpectations.associate(fixture.workload(), fixture.phase(), fixture.batches(), result.calls());
        var owner = result.samples().getFirst().identity();
        var deliveries = BenchmarkReturnTimeBounds.map(associations, owner, selected);
        assertThat(deliveries).hasSize(48_000);
        assertThat(BenchmarkReturnTimeBounds.p99(deliveries.stream().map(BenchmarkReturnTimeBounds.Delivery::latency).toList()))
                .isEqualTo(new BenchmarkCausalClock.Interval(bound(current, "p99LatencyNanos", "lowerNanos"),
                        bound(current, "p99LatencyNanos", "upperNanos")));
        assertThat(BenchmarkReturnCommonWindow.evidence(deliveries)).containsEntry("performanceAcceptanceEligible", false)
                .containsEntry("samplingCostQualified", false);
    }

    @Test void nominal_driver_dispatch_cannot_borrow_sample_coverage_or_accept_first_final_control() throws Exception {
        var fixture = fixture(); var result = fixture.result();
        var nominal = BenchmarkWriteReturnPhaseEvidenceTest.nominalClock(result);
        var samples = new ArrayList<>(result.samples()); var second = samples.get(1);
        samples.set(1, new BenchmarkCausalClock.Sample(second.sequence(), second.identity(), second.driverBeforeNanos(),
                second.driverAfterNanos() + 1, second.ownedNanos()));
        new BenchmarkCausalClock(second.identity(), samples);
        var changed = new BenchmarkWriteReturnCapture.Result(result.calls(), samples, result.summary(), result.pagesBase64());
        assertThatThrownBy(() -> record(fixture, changed, nominal)).isInstanceOf(AssertionError.class).hasMessageContaining("sample roster differs");
        var control = new BenchmarkWriteReturnCapture.Result(result.calls(), result.samples(), result.summary(), result.pagesBase64(),
                BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL);
        assertThatThrownBy(() -> record(fixture, control, nominal)).isInstanceOf(AssertionError.class).hasMessageContaining("complete periodic sample roster");
    }

    private static void assertAdmitted(PipelineBenchmarkComparison.Arm arm) {
        RealBenchmarkForkDriverIT.requireServices();
        assertThat(RealBenchmarkForkDriverIT.requireNominalReturnAdmission("stateless")).isTrue();
        var services = new AtomicInteger();
        RealBenchmarkForkDriverIT.requireRootCpuRunServices("stateless", services::incrementAndGet);
        assertThat(services).hasValue(1);
        assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(
                BenchmarkWorkloadDefinitions.steadyPilot("stateless"), arm, 1, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("application JAR");
    }

    private static void refuseBeforeServices(AtomicInteger services) {
        assertThatThrownBy(() -> RealBenchmarkForkDriverIT.requireRootCpuRunServices("stateless", services::incrementAndGet))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(
                BenchmarkWorkloadDefinitions.steadyPilot("stateless"), PipelineBenchmarkComparison.Arm.B, 1, null))
                .isInstanceOf(AssertionError.class);
        assertThat(services).hasValue(0);
    }

    private static BenchmarkWriteReturnPhaseEvidenceTest.Fixture fixture() throws Exception {
        return BenchmarkWriteReturnPhaseEvidenceTest.nativeFixture(BenchmarkWorkloadDefinitions.steadyPilot("stateless"));
    }

    private static Map<String, Object> record(BenchmarkWriteReturnPhaseEvidenceTest.Fixture fixture,
            BenchmarkWriteReturnCapture.Result result, BenchmarkReturnPointClock clock) {
        return RealBenchmarkForkDriver.recordWriteReturnPhase(fixture.workload(), fixture.phase(), fixture.batches(), result, clock);
    }

    @SuppressWarnings("unchecked") private static Map<String, Object> map(Map<String, Object> evidence, String key) {
        return (Map<String, Object>) evidence.get(key);
    }

    private static long bound(Map<String, Object> evidence, String field, String bound) {
        return ((Number) map(evidence, field).get(bound)).longValue();
    }

    private static final class Properties implements AutoCloseable {
        private final Map<String, String> previous = new LinkedHashMap<>();
        Properties() {
            for (String key : List.of(PROPERTY, RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY,
                    RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY,
                    RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY, RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY,
                    RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY, RealBenchmarkForkDriver.WRITE_RETURN_COST_STAGES_PROPERTY,
                    RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY, "tapstate.e2e.benchmark-smoke.arm",
                    "tapstate.e2e.benchmark-smoke.steady-pilot", "tapstate.e2e.benchmark-smoke.jar",
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
        void base() {
            clear(); System.setProperty(PROPERTY, "true");
            System.setProperty(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, "true");
            System.setProperty(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY, LIBRARY);
            System.setProperty("tapstate.e2e.benchmark-smoke.arm", "B");
            System.setProperty("tapstate.e2e.benchmark-smoke.steady-pilot", "true");
            System.setProperty("tapstate.e2e.benchmark-smoke.capture-mode", "PLAIN");
            System.setProperty("tapstate.e2e.benchmark-smoke.fork-output", "/private/tmp/nominal-return-admission-fork.json");
        }
        void nativeDomain(String arm) {
            base(); System.setProperty(RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY, "true");
            System.setProperty("tapstate.e2e.benchmark-smoke.arm", arm);
        }
        void collector(String mode) {
            base(); System.setProperty(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY, mode);
        }
        public void close() {
            previous.forEach((key, value) -> { if (value == null) { System.clearProperty(key); } else { System.setProperty(key, value); } });
        }
    }
}
