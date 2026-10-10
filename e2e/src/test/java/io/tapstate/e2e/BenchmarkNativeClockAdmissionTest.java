package io.tapstate.e2e;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkNativeClockAdmissionTest {
    private static final String LIBRARY = "/private/tmp/controlled-native-clock.dylib";

    @Test void native_clock_is_default_off_and_requires_the_original_isolated_diagnostic() {
        assertThat(RealBenchmarkForkDriver.nativeClockLibrary(null, false, false,
                BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL, true)).isNull();
        assertThat(RealBenchmarkForkDriver.nativeClockLibrary(LIBRARY, true, true,
                BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isEqualTo(LIBRARY);
        for (boolean diagnostics : new boolean[]{false, true}) {
            for (boolean pilot : new boolean[]{false, true}) {
                if (diagnostics && pilot) { continue; }
                assertThatThrownBy(() -> RealBenchmarkForkDriver.nativeClockLibrary(LIBRARY, diagnostics, pilot,
                        BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isInstanceOf(AssertionError.class);
            }
        }
        assertThatThrownBy(() -> RealBenchmarkForkDriver.nativeClockLibrary(LIBRARY, true, true,
                BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL, false)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> RealBenchmarkForkDriver.nativeClockLibrary(LIBRARY, true, true,
                BenchmarkReturnClockSampler.Mode.PERIODIC, true)).isInstanceOf(AssertionError.class);
        for (String value : List.of("", "relative.dylib", "/" + "x".repeat(512), "/line\npath")) {
            assertThatThrownBy(() -> RealBenchmarkForkDriver.nativeClockLibrary(value, true, true,
                    BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isInstanceOf(AssertionError.class);
        }
    }

    @Test void actual_child_arguments_select_the_same_library_without_other_controls() {
        assertThat(RealBenchmarkForkDriverIT.returnArguments(false, false, null))
                .containsExactly("-Dtapstate.benchmark.write-return=true");
        assertThat(RealBenchmarkForkDriverIT.returnArguments(false, false, LIBRARY)).containsExactly(
                "-Dtapstate.benchmark.write-return=true",
                "-Dtapstate.benchmark.native-clock-library=" + LIBRARY);
        assertThatThrownBy(() -> RealBenchmarkForkDriverIT.returnArguments(true, false, LIBRARY))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> RealBenchmarkForkDriverIT.returnArguments(false, true, LIBRARY))
                .isInstanceOf(AssertionError.class);
    }

    @Test void common_counter_mapping_is_default_off_and_requires_the_native_isolated_scope() {
        assertThat(RealBenchmarkForkDriver.nativeCounterDomain(null, null, false, false,
                BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL, true)).isFalse();
        assertThat(RealBenchmarkForkDriver.nativeCounterDomain("false", null, false, false,
                BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL, true)).isFalse();
        assertThat(RealBenchmarkForkDriver.nativeCounterDomain("true", LIBRARY, true, true,
                BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isTrue();
        for (String value : List.of("", "yes", "TRUE")) {
            assertThatThrownBy(() -> RealBenchmarkForkDriver.nativeCounterDomain(value, LIBRARY, true, true,
                    BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isInstanceOf(AssertionError.class);
        }
        assertThatThrownBy(() -> RealBenchmarkForkDriver.nativeCounterDomain("true", null, true, true,
                BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> RealBenchmarkForkDriver.nativeCounterDomain("true", LIBRARY, false, true,
                BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> RealBenchmarkForkDriver.nativeCounterDomain("true", LIBRARY, true, false,
                BenchmarkReturnClockSampler.Mode.PERIODIC, false)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> RealBenchmarkForkDriver.nativeCounterDomain("true", LIBRARY, true, true,
                BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL, false)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> RealBenchmarkForkDriver.nativeCounterDomain("true", LIBRARY, true, true,
                BenchmarkReturnClockSampler.Mode.PERIODIC, true)).isInstanceOf(AssertionError.class);
    }

    @Test void formal_entry_refuses_shared_mapping_before_configuration_without_a_library() {
        String property = RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY;
        String previous = System.getProperty(property);
        try {
            System.setProperty(property, "true");
            assertThatThrownBy(() -> new PipelineBenchmarkLiveRunIT()
                    .interleavedRealForksWriteEvidenceAndEnforceTheSelectedGate())
                    .isInstanceOf(AssertionError.class).hasMessageContaining("common native counter diagnostics cannot establish");
        } finally { restore(property, previous); }
    }

    @Test void driver_refuses_shared_mapping_without_native_before_missing_artifact_access() {
        String property = RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY;
        String previous = System.getProperty(property);
        try {
            System.setProperty(property, "true");
            assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(BenchmarkWorkloadDefinitions.steadyPilot("stateless"),
                    PipelineBenchmarkComparison.Arm.B, 1, Path.of("/missing-shared-counter.jar")))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("common native counter diagnostics require");
        } finally { restore(property, previous); }
    }

    @Test void formal_entry_refuses_native_diagnostics_before_configuration_or_fixtures() {
        String property = RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY;
        String previous = System.getProperty(property);
        try {
            System.setProperty(property, LIBRARY);
            assertThatThrownBy(() -> new PipelineBenchmarkLiveRunIT()
                    .interleavedRealForksWriteEvidenceAndEnforceTheSelectedGate())
                    .isInstanceOf(AssertionError.class).hasMessageContaining("native clock diagnostic cannot establish");
        } finally { restore(property, previous); }
    }

    @Test void nonpilot_driver_refuses_native_diagnostics_before_reading_a_missing_artifact() {
        String property = RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY;
        String previous = System.getProperty(property);
        String diagnostics = System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY);
        try {
            System.setProperty(property, LIBRARY);
            System.setProperty(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, "true");
            assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(BenchmarkWorkloadDefinitions.byId("stateless"),
                    PipelineBenchmarkComparison.Arm.B, 1, Path.of("/missing-native-clock.jar")))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("original plain stateless B");
        } finally {
            restore(property, previous);
            restore(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, diagnostics);
        }
    }

    private static void restore(String property, String previous) {
        if (previous == null) { System.clearProperty(property); } else { System.setProperty(property, previous); }
    }
}
