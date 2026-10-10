package io.tapstate.e2e;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The combined launch is bounded diagnostic evidence and requires its unchanged explicit scope. */
class BenchmarkWriteReturnJfrAdmissionTest {
    private static final Path OUTPUT = Path.of("/explicit/owned-fork.json");

    @Test void only_the_original_stateless_B_plain_periodic_scope_is_admitted() {
        assertThatCode(() -> RealBenchmarkForkDriverIT.requireWriteReturnJfrScope(scope(
                "stateless", PipelineBenchmarkComparison.Arm.B, BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN,
                OUTPUT, true, BenchmarkReturnClockSampler.Mode.PERIODIC, false))).doesNotThrowAnyException();
    }

    @Test void wrong_workload_arm_profile_output_or_clock_control_is_refused_before_launch() {
        var nonPlain = Arrays.stream(BenchmarkCaptureCalibrationLiveRunIT.Mode.values())
                .filter(mode -> mode != BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN).findFirst().orElseThrow();
        var B = PipelineBenchmarkComparison.Arm.B; var plain = BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN;
        var periodic = BenchmarkReturnClockSampler.Mode.PERIODIC;
        List<RealBenchmarkForkDriverIT.WriteReturnJfrScope> refused = List.of(
                scope("copy", B, plain, OUTPUT, true, periodic, false),
                scope("stateful", B, plain, OUTPUT, true, periodic, false),
                scope("stateless", PipelineBenchmarkComparison.Arm.A, plain, OUTPUT, true, periodic, false),
                scope("stateless", B, nonPlain, OUTPUT, true, periodic, false),
                scope("stateless", B, plain, OUTPUT, false, periodic, false),
                scope("stateless", B, plain, null, true, periodic, false),
                scope("stateless", B, plain, Path.of("relative.json"), true, periodic, false),
                scope("stateless", B, plain, OUTPUT, true, BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL, false),
                scope("stateless", B, plain, OUTPUT, true, periodic, true));
        for (var candidate : refused) {
            assertThatThrownBy(() -> RealBenchmarkForkDriverIT.requireWriteReturnJfrScope(candidate))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("original plain stateless B");
        }
    }

    @Test void original_JFR_options_are_preserved_and_only_the_dedicated_mode_enables_return_receipts() {
        Path settings = Path.of("/owned/settings.jfc"), recording = Path.of("/owned/runtime.jfr"), gc = Path.of("/owned/gc.log");
        var original = BenchmarkJvmDiagnostics.jvmOptions(settings, recording, gc, false);
        assertThat(original).containsExactly(
                "-Xlog:gc*,safepoint:file=/owned/gc.log:utctime,uptimenanos,level,tags:filecount=1,filesize=4m",
                "-XX:StartFlightRecording=filename=/owned/runtime.jfr,settings=/owned/settings.jfc,dumponexit=true,maxsize=64m");
        var combined = BenchmarkJvmDiagnostics.jvmOptions(settings, recording, gc, true);
        assertThat(combined.subList(0, original.size())).containsExactlyElementsOf(original);
        assertThat(combined).hasSize(3).containsOnlyOnce("-Dtapstate.benchmark.write-return=true");
        assertThatThrownBy(() -> combined.add("unowned option")).isInstanceOf(UnsupportedOperationException.class);
        assertThat(original).hasSize(2);
    }

    private static RealBenchmarkForkDriverIT.WriteReturnJfrScope scope(String workload, PipelineBenchmarkComparison.Arm arm,
            BenchmarkCaptureCalibrationLiveRunIT.Mode mode, Path output, boolean pilot,
            BenchmarkReturnClockSampler.Mode clock, boolean conflicting) {
        return new RealBenchmarkForkDriverIT.WriteReturnJfrScope(workload, arm, mode, output, pilot, clock, conflicting);
    }
}
