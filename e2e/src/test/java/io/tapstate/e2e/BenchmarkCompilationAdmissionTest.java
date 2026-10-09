package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Diagnostic counters cannot launch a formal comparison or alter another workload profile. */
class BenchmarkCompilationAdmissionTest {
    private static final String DIAGNOSTIC = "tapstate.e2e.benchmark.compilation-diagnostics";

    @Test
    void formalGateRefusesCompilationReadsBeforeRequiringFilesOrLaunchingProcesses() {
        withProperties(Map.of(DIAGNOSTIC, "true", "tapstate.e2e.benchmark.baseline-jar", "unused"), () ->
                assertThatThrownBy(() -> new PipelineBenchmarkLiveRunIT()
                        .interleavedRealForksWriteEvidenceAndEnforceTheSelectedGate())
                        .isInstanceOf(AssertionError.class)
                        .hasMessageContaining("cannot establish a live performance gate"));
    }

    @Test
    void smokeRefusesDiagnosticWithoutItsOwnedExactOutputBeforeLaunchingTheJar() {
        withProperties(Map.of(DIAGNOSTIC, "true", "tapstate.e2e.benchmark-smoke.jar", "unused.jar",
                "tapstate.e2e.benchmark-smoke.arm", "B", "tapstate.e2e.benchmark-smoke.steady-pilot", "true"), () ->
                assertThatThrownBy(() -> new RealBenchmarkForkDriverIT()
                        .copyForkUsesRealTargetChangesAndItsOwnTerminalPosition())
                        .isInstanceOf(AssertionError.class)
                        .hasMessageContaining("unchanged plain original copy B"));
    }

    private static void withProperties(Map<String, String> values, Runnable assertion) {
        Map<String, String> previous = new LinkedHashMap<>();
        values.forEach((key, value) -> previous.put(key, System.setProperty(key, value)));
        try {
            assertion.run();
        } finally {
            previous.forEach((key, value) -> {
                if (value == null) { System.clearProperty(key); }
                else { System.setProperty(key, value); }
            });
        }
    }
}
