package io.tapstate.core.alerting;

import io.tapstate.core.lifecycle.*;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.core.alerting.MetricAlertEvaluator.*;

class MetricAlertEvaluatorTest {
    static final Instant T = Instant.parse("2026-10-21T00:00:00Z");
    static Rule rule(MetricType type) {
        return new Rule("test.quantity", type, "1", Map.of(), Direction.ABOVE,
                BigDecimal.valueOf(10), BigDecimal.valueOf(5));
    }
    static Result read(Rule r, MetricPoint p, MetricPoint previous, boolean firing, int now) {
        return evaluate(r, MetricFact.single(r.name(), r.type(), r.unit(), p), previous, firing,
                T.plusSeconds(now), Duration.ofSeconds(15), Duration.ofSeconds(10));
    }
    @Test void gaugesUseOriginalFactsAndHysteresisWithoutAnyExporter() {
        Rule r = rule(MetricType.GAUGE);
        assertThat(read(r, MetricPoint.reading(Map.of(), T, 11), null, false, 0).condition()).isEqualTo(AlertSignal.Condition.BAD);
        assertThat(read(r, MetricPoint.reading(Map.of(), T, 10), null, false, 0).condition()).isEqualTo(AlertSignal.Condition.GOOD);
        assertThat(read(r, MetricPoint.reading(Map.of(), T, 5), null, true, 0).condition()).isEqualTo(AlertSignal.Condition.BAD);
        assertThat(read(r, MetricPoint.reading(Map.of(), T, 4), null, true, 0).condition()).isEqualTo(AlertSignal.Condition.GOOD);
        assertThat(read(r, MetricPoint.reading(Map.of(), T, 100), null, false, 16).quality()).isEqualTo(Quality.STALE);
        assertThat(read(r, MetricPoint.reading(Map.of(), T.plusSeconds(1), 0), null, true, 0).quality()).isEqualTo(Quality.CLOCK_INVALID);
    }
    @Test void counterRateNeverBridgesResetGapOrOldSamples() {
        Rule r = rule(MetricType.COUNTER);
        MetricPoint a = MetricPoint.accumulated(Map.of(), T.minusSeconds(10), T, 100);
        MetricPoint b = MetricPoint.accumulated(Map.of(), T.minusSeconds(10), T.plusSeconds(5), 200);
        assertThat(read(r, b, a, false, 5).value()).isEqualByComparingTo("20");
        assertThat(read(r, a, a, false, 0).quality()).isEqualTo(Quality.OUT_OF_ORDER);
        assertThat(read(r, MetricPoint.accumulated(Map.of(), T, T.plusSeconds(5), 200), a, false, 5).quality()).isEqualTo(Quality.RESET);
        assertThat(read(r, MetricPoint.accumulated(Map.of(), T.minusSeconds(10), T.plusSeconds(5), 50), a, false, 5).quality()).isEqualTo(Quality.RESET);
        assertThat(read(r, MetricPoint.accumulated(Map.of(), T.minusSeconds(10), T.plusSeconds(11), 200), a, false, 11).quality()).isEqualTo(Quality.GAP);
    }
    @Test void absenceOrDifferentUnitsCannotBecomeZeroOrAHealthySample() {
        Rule r = rule(MetricType.GAUGE);
        assertThat(evaluate(r, null, null, true, T, Duration.ofSeconds(10), Duration.ofSeconds(5)).condition()).isEqualTo(AlertSignal.Condition.UNKNOWN);
        MetricFact wrong = MetricFact.single(r.name(), MetricType.GAUGE, "s", MetricPoint.reading(Map.of(), T, 0));
        assertThat(evaluate(r, wrong, null, true, T, Duration.ofSeconds(10), Duration.ofSeconds(5)).quality()).isEqualTo(Quality.UNSUPPORTED);
    }
    @Test void independentFreshnessDetectsNoSamplesOnlyWhileObservationsAreExpected() {
        assertThat(ObservationFreshness.evaluate(true, T, null, T.plusSeconds(9), Duration.ofSeconds(10), Duration.ofSeconds(5))).isEqualTo(AlertSignal.Condition.UNKNOWN);
        assertThat(ObservationFreshness.evaluate(true, T, null, T.plusSeconds(10), Duration.ofSeconds(10), Duration.ofSeconds(5))).isEqualTo(AlertSignal.Condition.BAD);
        assertThat(ObservationFreshness.evaluate(false, T, T, T.plusSeconds(20), Duration.ofSeconds(10), Duration.ofSeconds(5))).isEqualTo(AlertSignal.Condition.UNKNOWN);
        assertThat(ObservationFreshness.evaluate(true, T, T.plusSeconds(19), T.plusSeconds(20), Duration.ofSeconds(10), Duration.ofSeconds(5))).isEqualTo(AlertSignal.Condition.GOOD);
        assertThat(ObservationFreshness.evaluate(true, T, T, T.plusSeconds(20), Duration.ofSeconds(10), Duration.ofSeconds(5))).isEqualTo(AlertSignal.Condition.BAD);
    }
    @Test void lowerThresholdRulesMirrorStrictTriggerAndRecoveryBoundaries() {
        Rule r = new Rule("test.quantity", MetricType.GAUGE, "1", Map.of(), Direction.BELOW,
                BigDecimal.valueOf(5), BigDecimal.valueOf(10));
        assertThat(read(r, MetricPoint.reading(Map.of(), T, 5), null, false, 0).condition()).isEqualTo(AlertSignal.Condition.GOOD);
        assertThat(read(r, MetricPoint.reading(Map.of(), T, 4), null, false, 0).condition()).isEqualTo(AlertSignal.Condition.BAD);
        assertThat(read(r, MetricPoint.reading(Map.of(), T, 10), null, true, 0).condition()).isEqualTo(AlertSignal.Condition.BAD);
        assertThat(read(r, MetricPoint.reading(Map.of(), T, 11), null, true, 0).condition()).isEqualTo(AlertSignal.Condition.GOOD);
    }
    @Test void metricThresholdsAndDurationsComposeWithoutEqualityOrIntermediateValueShortcuts() {
        Rule metric = new Rule("test.quantity", MetricType.GAUGE, "s", Map.of(), Direction.ABOVE,
                BigDecimal.valueOf(60), BigDecimal.valueOf(30));
        AlertRule lifecycle = new AlertRule(AlertLifecycleTest.key("org-a", "resource-1", "lag-rule"), 1, 1, "producer-1",
                Duration.ofSeconds(30), Duration.ofSeconds(20), Duration.ofSeconds(15), Duration.ofSeconds(10));
        AlertSnapshot state = AlertSnapshot.initial(lifecycle);
        int[] values = {59, 60, 61, 61, 60, 61, 61, 61, 61, 31, 29, 30, 29, 29, 29};
        for (int i = 0; i < values.length; i++) {
            int time = i * 10;
            MetricPoint point = MetricPoint.reading(Map.of(), T.plusSeconds(time), values[i]);
            var measured = evaluate(metric, MetricFact.single(metric.name(), metric.type(), metric.unit(), point), null,
                    state.status() == AlertSnapshot.Status.FIRING, T.plusSeconds(time), Duration.ofSeconds(15), Duration.ofSeconds(10));
            state = AlertLifecycle.evaluate(state, AlertLifecycleTest.signal(lifecycle, i + 1, time,
                    measured.condition(), AlarmSeverity.MAJOR), T.plusSeconds(time)).snapshot();
            if (i < 8) { assertThat(state.status()).isNotEqualTo(AlertSnapshot.Status.FIRING); }
            if (i >= 8 && i < 14) { assertThat(state.status()).isEqualTo(AlertSnapshot.Status.FIRING); }
        }
        assertThat(state.episode().firedAt()).isEqualTo(T.plusSeconds(80));
        assertThat(state.episode().recoveredAt()).isEqualTo(T.plusSeconds(140));
    }
}
