package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnForkBridgeTest {
    @Test void only_the_exact_explicit_method_selects_the_new_route() {
        assertThat(BenchmarkReturnForkBridge.selected(null)).isFalse();
        assertThat(BenchmarkReturnForkBridge.selected(BenchmarkReturnMethodAdmission.METHOD)).isTrue();
        for (String invalid : new String[]{"", "true", "false", "ACKNOWLEDGED_FULL_CALL_RETURN_NOMINAL_V1 ", "OPERATION_DATE_V9"})
            assertThatThrownBy(() -> BenchmarkReturnForkBridge.selected(invalid)).isInstanceOf(AssertionError.class);
    }

    @Test void absent_selector_keeps_the_original_unscoped_entry_without_acquisition() {
        String previous = System.getProperty(BenchmarkReturnForkBridge.PROPERTY);
        try {
            System.clearProperty(BenchmarkReturnForkBridge.PROPERTY);
            assertThat(BenchmarkReturnForkBridge.open("copy", null)).isNull();
            assertThat(BenchmarkReturnWorkBudgetRun.current()).isNull();
            assertThat(BenchmarkReturnWorkBudget.current()).isNull();
        } finally { restore(BenchmarkReturnForkBridge.PROPERTY, previous); }
    }

    @Test void an_explicit_selector_cannot_issue_a_token_without_the_actual_acquired_run() {
        String previous = System.getProperty(BenchmarkReturnForkBridge.PROPERTY);
        try {
            System.setProperty(BenchmarkReturnForkBridge.PROPERTY, BenchmarkReturnMethodAdmission.METHOD);
            assertThatThrownBy(() -> BenchmarkReturnForkBridge.open("stateless", null))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("active acquired Run");
            assertThat(BenchmarkReturnWorkBudget.current()).isNull();
        } finally { restore(BenchmarkReturnForkBridge.PROPERTY, previous); }
    }

    @Test void both_original_arms_use_the_explicit_accounted_mode_without_calibration() {
        for (String arm : new String[]{"A", "B"})
            assertThat(BenchmarkReturnWorkBudgetRun.accountedReturnCollectorMode(BenchmarkReturnMethodAdmission.METHOD,
                    null, arm, "true")).isEqualTo("ON");
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetRun.accountedReturnCollectorMode(BenchmarkReturnMethodAdmission.METHOD,
                "ON", "B", "true")).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetRun.accountedReturnCollectorMode(BenchmarkReturnMethodAdmission.METHOD,
                null, "B", "false")).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetRun.accountedReturnCollectorMode(BenchmarkReturnMethodAdmission.METHOD,
                null, "foreign", "true")).isInstanceOf(AssertionError.class);
    }

    @Test void binary_double_endpoints_are_outward_enclosed_instead_of_shortest_decimal_coercion() {
        double lower = 1.0842021724855044e-6, upper = Math.nextUp(lower);
        var bounds = BenchmarkReturnForkBridge.rateBounds(new BenchmarkReturnTimeBounds.Rate(lower, upper));
        assertThat(bounds.lower()).isLessThanOrEqualTo(new BigDecimal(lower));
        assertThat(bounds.upper()).isGreaterThanOrEqualTo(new BigDecimal(upper));
        assertThat(new BigDecimal(BenchmarkReturnForkBridge.decimal(bounds.lower()))).isEqualByComparingTo(bounds.lower());
        assertThat(new BigDecimal(BenchmarkReturnForkBridge.decimal(bounds.upper()))).isEqualByComparingTo(bounds.upper());
    }

    @Test void decimal_projection_has_a_prior_before_expanding_plain_text() {
        var wide = new BigDecimal("9".repeat(64)).scaleByPowerOfTen(64);
        assertThat(BenchmarkReturnForkBridge.decimal(wide)).hasSize(128);
        assertThatThrownBy(() -> BenchmarkReturnForkBridge.decimal(BigDecimal.ONE.scaleByPowerOfTen(100000)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("prior material");
        assertThatThrownBy(() -> BenchmarkReturnForkBridge.rateBounds(new BenchmarkReturnTimeBounds.Rate(0.1, Double.MAX_VALUE)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("material domain");
    }

    @Test void business_optimization_refuses_before_artifact_or_driver_access() {
        assertThatThrownBy(() -> PipelineBenchmarkReturnHarness.run(PipelineBenchmarkHarness.Gate.BUSINESS_OPTIMIZATION,
                Path.of("/unopened-a.jar"), Path.of("/unopened-b.jar"), null,
                (workload, arm, ordinal, app) -> { throw new AssertionError("driver must not be called"); }))
                .isInstanceOf(AssertionError.class).hasMessageContaining("business optimization remains unavailable");
    }

    @Test void an_incomplete_original_workload_roster_cannot_expose_an_interval_gate() {
        assertThatThrownBy(() -> PipelineBenchmarkReturnHarness.run(PipelineBenchmarkHarness.Gate.OBSERVABILITY_COST,
                Path.of("/unopened-a.jar"), Path.of("/unopened-b.jar"), java.util.List.of(),
                (workload, arm, ordinal, app) -> { throw new AssertionError("driver must not be called"); }))
                .isInstanceOf(AssertionError.class).hasMessageContaining("all three registered original workloads");
    }

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key); else System.setProperty(key, value);
    }
}
