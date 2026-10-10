package io.tapstate.e2e;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkWriteReturnMethodControlTest {
    @Test void both_explicit_on_and_off_protocol_slots_require_the_same_registered_scope() {
        for (String value : new String[]{"false", "true"}) {
            assertThat(RealBenchmarkForkDriver.admitWriteReturnMethodProtocol(value, true, true,
                    BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isEqualTo("true".equals(value));
            assertThatThrownBy(() -> RealBenchmarkForkDriver.admitWriteReturnMethodProtocol(value, false, true,
                    BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isInstanceOf(AssertionError.class);
            assertThatThrownBy(() -> RealBenchmarkForkDriver.admitWriteReturnMethodProtocol(value, true, true,
                    BenchmarkReturnClockSampler.Mode.PERIODIC, true)).isInstanceOf(AssertionError.class);
        }
    }
    @Test void disabling_the_method_skips_capture_even_when_native_diagnostics_stay_enabled() {
        assertThat(RealBenchmarkForkDriver.collectWriteReturns(true, false)).isTrue();
        assertThat(RealBenchmarkForkDriver.collectWriteReturns(true, true)).isFalse();
        assertThat(RealBenchmarkForkDriver.collectWriteReturns(false, false)).isFalse();
        assertThat(RealBenchmarkForkDriver.collectWriteReturns(false, true)).isFalse();
    }
    @Test void a_full_method_control_cannot_become_a_clock_only_control_or_unregistered_profile() {
        assertThat(RealBenchmarkForkDriver.writeReturnMethodControl("true", true, true,
                BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isTrue();
        assertThat(RealBenchmarkForkDriver.writeReturnMethodControl(null, false, false,
                BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isFalse();
        assertThat(RealBenchmarkForkDriver.writeReturnMethodControl("false", true, true,
                BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isFalse();
        for (boolean diagnostics : new boolean[]{false, true}) {
            for (boolean pilot : new boolean[]{false, true}) {
                if (diagnostics && pilot) { continue; }
                assertThatThrownBy(() -> RealBenchmarkForkDriver.writeReturnMethodControl("true", diagnostics, pilot,
                        BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isInstanceOf(AssertionError.class);
            }
        }
        assertThatThrownBy(() -> RealBenchmarkForkDriver.writeReturnMethodControl("true", true, true,
                BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL, false)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> RealBenchmarkForkDriver.writeReturnMethodControl("true", true, true,
                BenchmarkReturnClockSampler.Mode.PERIODIC, true)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> RealBenchmarkForkDriver.writeReturnMethodControl("off", true, true,
                BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("true or false");
    }

    @Test void formal_entry_refuses_method_control_before_accessing_configuration_or_fixtures() {
        String property = RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY;
        String previous = System.getProperty(property);
        try {
            System.setProperty(property, "true");
            assertThatThrownBy(() -> new PipelineBenchmarkLiveRunIT()
                    .interleavedRealForksWriteEvidenceAndEnforceTheSelectedGate())
                    .isInstanceOf(AssertionError.class).hasMessageContaining("return method cost control cannot establish");
        } finally { restore(property, previous); }
    }

    @Test void nonpilot_driver_control_is_refused_before_reading_a_missing_artifact() {
        String property = RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY;
        String previous = System.getProperty(property);
        String diagnostics = System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY);
        try {
            System.setProperty(property, "true");
            System.setProperty(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, "true");
            assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(BenchmarkWorkloadDefinitions.byId("stateless"),
                    PipelineBenchmarkComparison.Arm.B, 1, Path.of("/missing-method-control.jar")))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("original plain diagnostic pilot");
        } finally {
            restore(property, previous);
            restore(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, diagnostics);
        }
    }

    private static void restore(String property, String previous) {
        if (previous == null) { System.clearProperty(property); } else { System.setProperty(property, previous); }
    }
}
