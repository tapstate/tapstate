package io.tapstate.e2e;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkWriteReturnCostStageControlTest {
    @Test void cost_stages_are_default_off_and_require_the_original_isolated_diagnostic() {
        assertThat(RealBenchmarkForkDriver.writeReturnCostStages(null, false, false,
                BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL, true)).isFalse();
        assertThat(RealBenchmarkForkDriver.writeReturnCostStages("false", false, false,
                BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL, true)).isFalse();
        assertThat(RealBenchmarkForkDriver.writeReturnCostStages("true", true, true,
                BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isTrue();
        for (boolean diagnostics : new boolean[]{false, true}) {
            for (boolean pilot : new boolean[]{false, true}) {
                if (diagnostics && pilot) { continue; }
                assertThatThrownBy(() -> RealBenchmarkForkDriver.writeReturnCostStages("true", diagnostics, pilot,
                        BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isInstanceOf(AssertionError.class);
            }
        }
        assertThatThrownBy(() -> RealBenchmarkForkDriver.writeReturnCostStages("true", true, true,
                BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL, false)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> RealBenchmarkForkDriver.writeReturnCostStages("true", true, true,
                BenchmarkReturnClockSampler.Mode.PERIODIC, true)).isInstanceOf(AssertionError.class);
        for (String value : List.of("TRUE", "yes", "")) {
            assertThatThrownBy(() -> RealBenchmarkForkDriver.writeReturnCostStages(value, true, true,
                    BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("true or false");
        }
    }

    @Test void child_arguments_enable_stages_only_with_the_same_enabled_producer() {
        assertThat(RealBenchmarkForkDriver.returnJvmArguments(false, false))
                .containsExactly("-Dtapstate.benchmark.write-return=true");
        assertThat(RealBenchmarkForkDriver.returnJvmArguments(true, false))
                .containsExactly("-Dtapstate.benchmark.write-return=false");
        assertThat(RealBenchmarkForkDriver.returnJvmArguments(false, true)).containsExactly(
                "-Dtapstate.benchmark.write-return=true", "-Dtapstate.benchmark.write-return-cost-stages=true");
        assertThatThrownBy(() -> RealBenchmarkForkDriver.returnJvmArguments(true, true)).isInstanceOf(AssertionError.class);
    }

    @Test void formal_entry_refuses_stage_diagnostics_before_reading_configuration_or_fixtures() {
        String property = RealBenchmarkForkDriver.WRITE_RETURN_COST_STAGES_PROPERTY;
        String previous = System.getProperty(property);
        try {
            System.setProperty(property, "true");
            assertThatThrownBy(() -> new PipelineBenchmarkLiveRunIT()
                    .interleavedRealForksWriteEvidenceAndEnforceTheSelectedGate())
                    .isInstanceOf(AssertionError.class).hasMessageContaining("cost-stage diagnostics cannot establish");
        } finally { restore(property, previous); }
    }

    @Test void a_nonpilot_driver_is_refused_before_reading_a_missing_artifact() {
        String property = RealBenchmarkForkDriver.WRITE_RETURN_COST_STAGES_PROPERTY;
        String previous = System.getProperty(property);
        String diagnostics = System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY);
        try {
            System.setProperty(property, "true");
            System.setProperty(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, "true");
            assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(BenchmarkWorkloadDefinitions.byId("stateless"),
                    PipelineBenchmarkComparison.Arm.B, 1, Path.of("/missing-cost-stages.jar")))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("original plain stateless B diagnostic pilot");
        } finally {
            restore(property, previous);
            restore(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, diagnostics);
        }
    }

    private static void restore(String property, String previous) {
        if (previous == null) { System.clearProperty(property); } else { System.setProperty(property, previous); }
    }
}
