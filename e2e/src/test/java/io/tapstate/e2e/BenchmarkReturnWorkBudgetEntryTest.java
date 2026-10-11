package io.tapstate.e2e;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnWorkBudgetEntryTest {
    private static final String PROPERTY = RealBenchmarkForkDriver.WORK_BUDGET_DIAGNOSTICS_PROPERTY;

    @Test void absent_selection_does_not_require_a_budget_scope() {
        try (var properties = new Properties()) {
            assertThat(RealBenchmarkForkDriver.workBudgetDiagnostics(null, false)).isFalse();
            assertThat(BenchmarkReturnWorkBudget.current()).isNull();
        }
    }

    @Test void false_invalid_or_missing_nominal_method_refuse_before_service_access() {
        try (var properties = new Properties()) {
            for (String value : List.of("false", "TRUE", "invalid", "", "true")) {
                properties.clear(); System.setProperty(PROPERTY, value);
                assertThatThrownBy(RealBenchmarkForkDriverIT::requireServices).isInstanceOf(AssertionError.class)
                        .hasMessageContaining("work-budget diagnostics");
                assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(BenchmarkWorkloadDefinitions.steadyPilot("stateless"),
                        PipelineBenchmarkComparison.Arm.B, 1, null)).isInstanceOf(AssertionError.class)
                        .hasMessageContaining("work-budget diagnostics");
            }
        }
    }

    @Test void selecting_a_nominal_method_still_requires_the_registered_budget_before_fixture_access() {
        try (var properties = new Properties()) {
            System.setProperty(PROPERTY, "true");
            System.setProperty(RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY, "true");
            assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(BenchmarkWorkloadDefinitions.steadyPilot("stateless"),
                    PipelineBenchmarkComparison.Arm.B, 1, null)).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("scope before fixture access");
        }
    }

    @Test void every_explicit_value_is_refused_by_formal_entry_before_configuration() {
        try (var properties = new Properties()) {
            for (String value : List.of("true", "false", "invalid", "")) {
                properties.clear(); System.setProperty(PROPERTY, value);
                assertThatThrownBy(() -> new PipelineBenchmarkLiveRunIT().interleavedRealForksWriteEvidenceAndEnforceTheSelectedGate())
                        .isInstanceOf(AssertionError.class).hasMessageContaining("work-budget diagnostics");
            }
        }
    }

    @Test void absent_child_input_preserves_existing_arguments_and_enabled_input_is_explicit() {
        String library = "/private/tmp/controlled-work-budget-clock.dylib";
        assertThat(RealBenchmarkForkDriverIT.returnArguments(false, false, library, null))
                .isEqualTo(RealBenchmarkForkDriverIT.returnArguments(false, false, library));
        var input = new BenchmarkReturnWorkBudgetDefinitions.ProducerInput("controlled-run", "a".repeat(64),
                new BenchmarkReturnWorkBudget.Work(1, 2, 3, 4, 5, 6, 7, 8), 1024, 12, 2048);
        var arguments = RealBenchmarkForkDriverIT.returnArguments(false, false, library, input);
        assertThat(arguments).contains("-Dtapstate.benchmark.write-return-work-budget=true",
                "-Dtapstate.benchmark.write-return-work-budget-input=" + input.encoded());
        assertThatThrownBy(() -> RealBenchmarkForkDriverIT.returnArguments(true, false, library, input))
                .isInstanceOf(AssertionError.class);
    }

    private static final class Properties implements AutoCloseable {
        private final Map<String, String> before = new LinkedHashMap<>();
        private Properties() {
            for (String key : List.of(PROPERTY, RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY,
                    RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY, RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY)) {
                before.put(key, System.getProperty(key));
            }
            clear();
        }
        private void clear() { before.keySet().forEach(System::clearProperty); }
        @Override public void close() {
            before.forEach((key, value) -> { if (value == null) { System.clearProperty(key); } else { System.setProperty(key, value); } });
        }
    }
}
