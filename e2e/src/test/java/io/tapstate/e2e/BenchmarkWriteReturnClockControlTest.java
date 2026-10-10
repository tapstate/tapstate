package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkWriteReturnClockControlTest {
    @Test
    void controlRequiresExplicitReturnDiagnosticsAndTheOriginalPilot() {
        assertThat(RealBenchmarkForkDriver.writeReturnClockMode("true", true, true))
                .isEqualTo(BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL);
        for (boolean diagnostics : new boolean[]{false, true}) {
            for (boolean pilot : new boolean[]{false, true}) {
                if (diagnostics && pilot) { continue; }
                assertThatThrownBy(() -> RealBenchmarkForkDriver.writeReturnClockMode("true", diagnostics, pilot))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("original return diagnostic pilot");
            }
        }
        assertThat(RealBenchmarkForkDriver.writeReturnClockMode(null, false, false))
                .isEqualTo(BenchmarkReturnClockSampler.Mode.PERIODIC);
        assertThat(RealBenchmarkForkDriver.writeReturnClockMode("false", true, true))
                .isEqualTo(BenchmarkReturnClockSampler.Mode.PERIODIC);
        assertThatThrownBy(() -> RealBenchmarkForkDriver.writeReturnClockMode("boundary", true, true))
                .isInstanceOf(AssertionError.class).hasMessageContaining("true or false");
    }

    @Test
    void driverRefusesANonpilotControlBeforeAccessingItsMissingArtifactOrFixture() {
        String control = System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY);
        String diagnostics = System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY);
        try {
            System.setProperty(RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY, "true");
            System.setProperty(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, "true");
            assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(BenchmarkWorkloadDefinitions.byId("copy"),
                    PipelineBenchmarkComparison.Arm.B, 1, Path.of("/missing-clock-control-application.jar")))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("original return diagnostic pilot");
        } finally {
            restore(RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY, control);
            restore(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, diagnostics);
        }
    }

    @Test
    void formalEntryRefusesTheControlBeforeConfigurationAndFixtureAccess() {
        String control = System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY);
        try {
            System.setProperty(RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY, "true");
            assertThatThrownBy(() -> new PipelineBenchmarkLiveRunIT()
                    .interleavedRealForksWriteEvidenceAndEnforceTheSelectedGate())
                    .isInstanceOf(AssertionError.class).hasMessageContaining("cost control cannot establish a live performance gate");
        } finally {
            restore(RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY, control);
        }
    }

    private static void restore(String key, String value) {
        if (value == null) { System.clearProperty(key); } else { System.setProperty(key, value); }
    }
}
