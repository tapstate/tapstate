package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Sparse stack points cannot launch a formal comparison or replace another workload profile. */
class BenchmarkThreadPointAdmissionTest {
    private static final String ENABLED = "tapstate.e2e.benchmark.thread-point-diagnostics";
    private static final String SMOKE = "tapstate.e2e.benchmark-smoke.";

    @Test
    void formalEntryRefusesThreadPointsBeforeReadingJarOrOutputFiles() {
        withProperties(Map.of(ENABLED, "true", "tapstate.e2e.benchmark.baseline-jar", "unused.jar",
                "tapstate.e2e.benchmark.compilation-diagnostics", "false"), () ->
                assertThatThrownBy(() -> new PipelineBenchmarkLiveRunIT()
                        .interleavedRealForksWriteEvidenceAndEnforceTheSelectedGate())
                        .isInstanceOf(AssertionError.class).hasMessageContaining("thread point diagnostics cannot establish"));
    }

    @Test
    void smokeRequiresItsExactOwnedOutputBeforeLaunchingTheApplication() {
        withProperties(original(), () ->
                assertThatThrownBy(() -> new RealBenchmarkForkDriverIT()
                        .copyForkUsesRealTargetChangesAndItsOwnTerminalPosition())
                        .isInstanceOf(AssertionError.class).hasMessageContaining("unchanged plain original copy B"));
    }

    @Test
    void anUnchangedOutputPathCannotPermitAnotherWorkloadOrAChangedWarmup(@TempDir Path directory) {
        Path output = directory.resolve("uncreated-fork.json").toAbsolutePath();
        Map<String, String> otherWorkload = new LinkedHashMap<>(original());
        otherWorkload.put(SMOKE + "fork-output", output.toString());
        withProperties(otherWorkload, () ->
                assertThatThrownBy(() -> new RealBenchmarkForkDriverIT()
                        .statelessForkUsesPgoutputAndItsOwnTerminalPosition())
                        .isInstanceOf(AssertionError.class).hasMessageContaining("unchanged plain original copy B"));
        Map<String, String> changedWarmup = new LinkedHashMap<>(otherWorkload);
        changedWarmup.put(SMOKE + "full-cdc-settling-calibration", "true");
        withProperties(changedWarmup, () ->
                assertThatThrownBy(() -> new RealBenchmarkForkDriverIT()
                        .copyForkUsesRealTargetChangesAndItsOwnTerminalPosition())
                        .isInstanceOf(AssertionError.class).hasMessageContaining("unchanged plain original copy B"));
        assertThat(Files.exists(output)).isFalse();
    }

    private static Map<String, String> original() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(ENABLED, "true");
        properties.put(SMOKE + "jar", "unused.jar");
        properties.put(SMOKE + "arm", "B");
        properties.put(SMOKE + "capture-mode", "PLAIN");
        properties.put(SMOKE + "steady-pilot", "true");
        properties.put("tapstate.e2e.benchmark.load-diagnostics", "true");
        properties.put("tapstate.e2e.benchmark.compilation-diagnostics", "false");
        for (String option : new String[] {"jvm-gap-diagnostics", "dual-gc-diagnostics", "paced-calibration",
                "cdc-settling-calibration", "full-cdc-settling-calibration"}) {
            properties.put(SMOKE + option, "false");
        }
        properties.put(SMOKE + "fork-output", null);
        return properties;
    }

    private static void withProperties(Map<String, String> values, Runnable assertion) {
        Map<String, String> previous = new LinkedHashMap<>();
        values.forEach((key, value) -> {
            previous.put(key, System.getProperty(key));
            if (value == null) { System.clearProperty(key); }
            else { System.setProperty(key, value); }
        });
        try { assertion.run(); }
        finally {
            previous.forEach((key, value) -> {
                if (value == null) { System.clearProperty(key); }
                else { System.setProperty(key, value); }
            });
        }
    }
}
