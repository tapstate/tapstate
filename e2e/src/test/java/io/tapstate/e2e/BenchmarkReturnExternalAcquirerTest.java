package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnExternalAcquirerTest {
    @Test void both_arms_have_the_same_explicit_noncalibration_route() {
        for (String arm : List.of("A", "B")) {
            var flags = BenchmarkReturnExternalAcquirer.registeredFlags(registration(arm));
            assertThat(flags.arm()).isEqualTo(arm);
            assertThat(flags.collectorMode()).isEqualTo("ON");
            assertThat(flags.otherControls()).isEmpty();
            assertThat(flags.rootCpu()).isFalse();
        }
    }
    @Test void old_calibration_and_an_absent_formal_selector_do_not_gain_completion_authority() {
        var registration = registration("B");
        var properties = properties(registration);
        properties.put(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY, "ON");
        assertThatThrownBy(() -> BenchmarkReturnExternalAcquirer.registeredFlags(registration)).isInstanceOf(AssertionError.class);
        properties.remove(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY);
        properties.remove(BenchmarkReturnForkBridge.PROPERTY);
        assertThatThrownBy(() -> BenchmarkReturnExternalAcquirer.registeredFlags(registration)).isInstanceOf(AssertionError.class);
    }
    @Test void diagnostic_controls_and_qualification_claims_are_refused() {
        var registration = registration("A");
        properties(registration).put(BenchmarkWitnessReadGate.PROPERTY, "false");
        assertThatThrownBy(() -> BenchmarkReturnExternalAcquirer.registeredFlags(registration)).isInstanceOf(AssertionError.class);
        properties(registration).remove(BenchmarkWitnessReadGate.PROPERTY);
        @SuppressWarnings("unchecked") var flags = (Map<String, Object>) registration.get("qualificationFlags");
        flags.put("formalPerformance", true);
        assertThatThrownBy(() -> BenchmarkReturnExternalAcquirer.registeredFlags(registration)).isInstanceOf(AssertionError.class);
    }
    @Test void stateful_packets_require_every_original_phase_cut_and_same_producer() {
        var owner = new BenchmarkCausalClock.Identity(42, 100);
        var phases = List.of("stateful/cold-read", "stateful/cdc-update");
        var packets = new ArrayList<Map<String, Object>>(List.of(packet("stateful/cold-read", "BEFORE", owner),
                packet("stateful/cold-read", "AFTER", owner), packet("stateful/cdc-update", "BEFORE", owner), packet("stateful/cdc-update", "AFTER", owner)));
        BenchmarkReturnExternalAcquirer.verifyOriginalPacketRoster(packets, phases, owner);
        assertThatThrownBy(() -> BenchmarkReturnExternalAcquirer.verifyOriginalPacketRoster(packets.subList(0, 2), phases, owner))
                .isInstanceOf(AssertionError.class);
        packets.set(2, packet("stateful/cold-read", "BEFORE", owner));
        assertThatThrownBy(() -> BenchmarkReturnExternalAcquirer.verifyOriginalPacketRoster(packets, phases, owner))
                .isInstanceOf(AssertionError.class);
        packets.set(2, packet("stateful/cdc-update", "BEFORE", new BenchmarkCausalClock.Identity(43, 100)));
        assertThatThrownBy(() -> BenchmarkReturnExternalAcquirer.verifyOriginalPacketRoster(packets, phases, owner))
                .isInstanceOf(AssertionError.class);
        packets.set(2, packet("stateful/cdc-update", "AFTER", owner));
        assertThatThrownBy(() -> BenchmarkReturnExternalAcquirer.verifyOriginalPacketRoster(packets, phases, owner))
                .isInstanceOf(AssertionError.class);
    }
    @Test void a_null_or_open_prefix_never_satisfies_completion() {
        assertThatThrownBy(() -> BenchmarkReturnCompletedMethod.verify(null)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("actual acquired completion is missing");
    }
    @Test void full_stateful_coverage_material_includes_both_phases_and_terminals() {
        var workload = BenchmarkWorkloadDefinitions.steadyPilot("stateful");
        long measured = workload.phases().stream().filter(BenchmarkWorkloadDefinitions.Phase::measured)
                .mapToLong(BenchmarkWorkloadDefinitions.Phase::expectedLogicalOutputChanges).sum();
        assertThat(measured).isEqualTo(288000);
        assertThat(BenchmarkReturnCompletedMethod.logicalCoverageUpper(workload)).isGreaterThan(measured);
    }
    @Test void measurement_uses_the_same_authenticated_native_packet_for_each_phase_and_cut() {
        var phases = List.of("stateful/cold-read", "stateful/cdc-update");
        var packets = new ArrayList<Map<String, Object>>(); var windows = new ArrayList<Map<String, Object>>();
        for (String phase : phases) {
            var clocks = new LinkedHashMap<String, Object>();
            for (String cut : List.of("BEFORE", "AFTER")) {
                String raw = phase + "/" + cut;
                var parts = new ArrayList<String>(java.util.Collections.nCopies(BenchmarkReturnProducerOrigins.PARTS, "controlled-origin"));
                parts.set(0, raw); var origins = Map.of("rawParts", List.copyOf(parts));
                packets.add(Map.of("phaseKey", phase, "cut", cut, "producerOrigins", origins));
                clocks.put(cut.equals("BEFORE") ? "nativeClockBefore" : "nativeClockAfter", Map.of("raw", raw, "producerOrigins", origins));
            }
            windows.add(Map.of("id", phase.substring(phase.indexOf('/') + 1), "targetClockQualification", clocks));
        }
        BenchmarkReturnExternalAcquirer.verifyMeasurementNativePackets(windows, packets, phases);
        @SuppressWarnings("unchecked") var secondClocks = (Map<String, Object>) windows.get(1).get("targetClockQualification");
        @SuppressWarnings("unchecked") var firstClocks = (Map<String, Object>) windows.get(0).get("targetClockQualification");
        secondClocks.put("nativeClockBefore", firstClocks.get("nativeClockBefore"));
        assertThatThrownBy(() -> BenchmarkReturnExternalAcquirer.verifyMeasurementNativePackets(windows, packets, phases))
                .isInstanceOf(AssertionError.class).hasMessageContaining("authenticated phase/cut packet");
    }
    private static Map<String, Object> packet(String phase, String cut, BenchmarkCausalClock.Identity owner) {
        return Map.of("phaseKey", phase, "cut", cut, "pid", owner.pid(), "jvmStartTimeMillis", owner.jvmStartTimeMillis());
    }
    private static Map<String, Object> registration(String arm) {
        var properties = new LinkedHashMap<String, Object>();
        properties.put("tapstate.e2e.benchmark-smoke.arm", arm);
        properties.put("tapstate.e2e.benchmark-smoke.capture-mode", "PLAIN");
        properties.put("tapstate.e2e.benchmark-smoke.steady-pilot", "true");
        properties.put(BenchmarkReturnForkBridge.PROPERTY, BenchmarkReturnMethodAdmission.METHOD);
        for (String key : List.of(RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY,
                RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY,
                RealBenchmarkForkDriver.WORK_BUDGET_DIAGNOSTICS_PROPERTY)) properties.put(key, "true");
        properties.put(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY, "/controlled/clock.dylib");
        properties.put("tapstate.e2e.benchmark-smoke.fork-output", "/controlled/fork.json");
        var flags = new LinkedHashMap<String, Object>(); BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> flags.put(flag, false));
        var registration = new LinkedHashMap<String, Object>(); registration.put("properties", properties);
        registration.put("qualificationFlags", flags); registration.put("otherControls", List.of()); registration.put("rootCpu", false);
        return registration;
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> properties(Map<String, Object> registration) {
        return (Map<String, Object>) registration.get("properties");
    }
}
