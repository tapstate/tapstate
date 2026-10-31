package io.tapstate.core.alerting;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.*;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.alerting.ObservationMetricInput.*;
import static org.assertj.core.api.Assertions.*;

class ObservationMetricInputTest {
    static final Instant T = Instant.parse("2026-10-21T00:00:00Z");
    static final Budget BUDGET = new Budget(4, 2048);
    static final AlertRule RULE = AlertLifecycleTest.rule(1, 10, 10);
    static final MetricAlertEvaluator.Rule METRIC = metric(Map.of());

    static MetricAlertEvaluator.Rule metric(Map<String, String> attributes) {
        return new MetricAlertEvaluator.Rule("tapstate.pipeline.lag", MetricType.GAUGE, "s", attributes,
                MetricAlertEvaluator.Direction.ABOVE, BigDecimal.TEN, BigDecimal.valueOf(5));
    }
    static MetricFact fact(Instant at, long value) {
        return MetricFact.single(METRIC.name(), METRIC.type(), METRIC.unit(), MetricPoint.reading(Map.of(), at, value));
    }
    static Observation observation(String id, Instant at, List<MetricFact> facts) {
        // The old numeric projection deliberately disagrees: the input must consume facts only.
        return new Observation(id, PipelineState.RUNNING, Map.of(METRIC.name(), 0L),
                Map.of(), Map.of(), null, at, facts);
    }
    static Reading read(AlertRule expected, Publication publication, Budget budget,
            MetricAlertEvaluator.Rule metric, boolean firing, Instant now) {
        return evaluate(expected, metric, publication, budget, null, firing, AlarmSeverity.MAJOR, now);
    }
    static Reading read(Observation observation, long sequence, Instant now) {
        return read(RULE, new Publication(RULE, sequence, observation), BUDGET, METRIC, false, now);
    }

    @Test void originalPointIdentityAndMeasurementTimeSurviveThePublishedProjection() {
        MetricFact original = fact(T, 11);
        Observation published = observation("orders", T.plusSeconds(2), List.of(original));
        Reading reading = read(published, 7, T.plusSeconds(3));
        assertThat(reading.status()).isEqualTo(Status.READY);
        assertThat(reading.point()).isSameAs(original.points().getFirst());
        assertThat(reading.signal()).isEqualTo(new AlertSignal(RULE.key(), 1, 1, "producer-1", 7,
                T, AlertSignal.Condition.BAD, AlarmSeverity.MAJOR));
        assertThat(reading.evaluation().value()).isEqualByComparingTo("11");
        assertThat(published.metrics().get(METRIC.name())).isZero();
    }

    @Test void realExportCardinalityFoldingCannotHideOrInventAResourceMeasurement() {
        List<MetricPoint> points = new ArrayList<>();
        // One point beyond the registered 1000-table fold budget is needed to distinguish the paths.
        for (int i = 0; i <= CardinalityBudget.LAG.distinctValues(); i++) {
            points.add(MetricPoint.reading(Map.of(MetricAttributes.TABLE_ID, "table-" + i), T, i == 1000 ? 11 : 0));
        }
        MetricFact original = new MetricFact(METRIC.name(), METRIC.type(), METRIC.unit(), points);
        MetricFact exported = CardinalityBudget.folder().fold(original);
        var target = metric(Map.of(MetricAttributes.TABLE_ID, "table-1000"));
        assertThat(exported.points()).noneMatch(p -> p.attributes().equals(target.attributes()));
        assertThat(exported.points()).anyMatch(p -> p.attributes().containsKey(MetricAttributes.OVERFLOW));
        Reading raw = read(RULE, new Publication(RULE, 1, observation("orders", T, List.of(original))),
                BUDGET, target, false, T);
        Reading folded = read(RULE, new Publication(RULE, 1, observation("orders", T, List.of(exported))),
                BUDGET, target, false, T);
        assertThat(raw.signal().condition()).isEqualTo(AlertSignal.Condition.BAD);
        assertThat(raw.point()).isSameAs(points.getLast());
        assertThat(folded.signal().condition()).isEqualTo(AlertSignal.Condition.UNKNOWN);
        assertThat(folded.evaluation().quality()).isEqualTo(MetricAlertEvaluator.Quality.MISSING);
        assertThat(original.points()).hasSize(1001);
    }

    @Test void exportOverflowIsNeverEvidenceForTheBoundResourceEvenWithAnExactSelector() {
        Map<String, String> aggregate = Map.of(MetricAttributes.OVERFLOW, "true");
        MetricFact folded = MetricFact.single(METRIC.name(), MetricType.GAUGE, "s",
                MetricPoint.reading(aggregate, T, 0));
        Reading reading = read(RULE, new Publication(RULE, 1, observation("orders", T, List.of(folded))),
                BUDGET, metric(aggregate), true, T);
        assertThat(reading.signal()).isNull();
        assertThat(reading.point()).isNull();
        assertThat(reading.status()).isNotEqualTo(Status.READY);
    }

    @Test void wholeInputBudgetsAreInclusiveAndNeverAcceptAMatchingPrefix() {
        MetricFact wanted = fact(T, 11);
        MetricFact other = MetricFact.single("other.quantity", MetricType.GAUGE, "1", MetricPoint.reading(Map.of(), T, 0));
        Observation full = observation("orders", T, List.of(wanted, other));
        assertThat(read(RULE, new Publication(RULE, 1, full), new Budget(2, 2), METRIC, false, T).status()).isEqualTo(Status.READY);
        for (Budget budget : List.of(new Budget(1, 2), new Budget(2, 1))) {
            Reading refused = read(RULE, new Publication(RULE, 1, full), budget, METRIC, false, T);
            assertThat(refused.status()).isEqualTo(budget.maxFacts() == 1 ? Status.FACT_LIMIT : Status.POINT_LIMIT);
            assertThat(refused.signal()).isNull();
            assertThat(refused.point()).isNull();
            assertThat(refused.evaluation()).isNull();
        }
        assertThatThrownBy(() -> new Budget(0, 1)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new Budget(1, 65537)).isInstanceOf(TapstateException.class);
    }

    @Test void scopeResourceIncarnationAndEvaluationBindingComeFromTheHost() {
        Observation published = observation("orders", T, List.of(fact(T, 11)));
        AlarmKey foreignScope = new AlarmKey(new AlarmKey.Scope("another-org", "cluster"), RULE.key().resource(),
                RULE.key().typeId(), RULE.key().qualifier());
        AlarmKey replacementResource = new AlarmKey(RULE.key().scope(), new AlarmKey.Resource("pipeline", "orders", "new-resource"),
                RULE.key().typeId(), RULE.key().qualifier());
        for (AlertRule old : List.of(withKey(foreignScope), withKey(replacementResource), AlertLifecycleTest.rule(2, 10, 10),
                new AlertRule(RULE.key(), 2, 1, "producer-1", RULE.triggerFor(), RULE.resolveFor(), RULE.maxAge(), RULE.maxGap()))) {
            assertThat(read(RULE, new Publication(old, 1, published), BUDGET, METRIC, false, T).status()).isEqualTo(Status.BINDING_MISMATCH);
        }
        Reading wrongPipeline = read(observation("another-pipeline", T, List.of(fact(T, 11))), 1, T);
        assertThat(wrongPipeline.status()).isEqualTo(Status.BINDING_MISMATCH);
        assertThat(wrongPipeline.signal()).isNull();
    }
    static AlertRule withKey(AlarmKey key) {
        return new AlertRule(key, RULE.revision(), RULE.generation(), RULE.producerIncarnation(),
                RULE.triggerFor(), RULE.resolveFor(), RULE.maxAge(), RULE.maxGap());
    }

    @Test void missingFactsAndSeriesStayUnknownAndNeverReadTheFlatMap() {
        Reading noFact = read(observation("orders", T, List.of()), 1, T);
        assertThat(noFact.status()).isEqualTo(Status.READY);
        assertThat(noFact.signal().condition()).isEqualTo(AlertSignal.Condition.UNKNOWN);
        assertThat(noFact.signal().observedAt()).isEqualTo(T);
        assertThat(noFact.evaluation().value()).isNull();
        Reading noSeries = read(RULE, new Publication(RULE, 1, observation("orders", T, List.of(fact(T, 11)))),
                BUDGET, metric(Map.of(MetricAttributes.TABLE_ID, "absent")), true, T);
        assertThat(noSeries.signal().condition()).isEqualTo(AlertSignal.Condition.UNKNOWN);
        MetricFact wrongUnit = MetricFact.single(METRIC.name(), MetricType.GAUGE, "ms", MetricPoint.reading(Map.of(), T, 0));
        assertThat(read(observation("orders", T, List.of(wrongUnit)), 1, T).evaluation().quality())
                .isEqualTo(MetricAlertEvaluator.Quality.UNSUPPORTED);
    }

    @Test void absentSeriesDoesNotEraseThePresentInstrumentsUnitMismatch() {
        MetricFact wrongUnit = MetricFact.single(METRIC.name(), MetricType.GAUGE, "ms",
                MetricPoint.reading(Map.of(), T, 0));
        Reading reading = read(RULE, new Publication(RULE, 1, observation("orders", T, List.of(wrongUnit))),
                BUDGET, metric(Map.of(MetricAttributes.TABLE_ID, "absent")), true, T);
        assertThat(reading.signal().condition()).isEqualTo(AlertSignal.Condition.UNKNOWN);
        assertThat(reading.evaluation().quality()).isEqualTo(MetricAlertEvaluator.Quality.UNSUPPORTED);
        assertThat(reading.point()).isNull();
    }

    @Test void missingOrInconsistentPublicationTimeCannotBecomeANewSample() {
        assertThat(read(observation("orders", null, List.of(fact(T, 11))), 1, T).status()).isEqualTo(Status.MISSING_OBSERVATION_TIME);
        assertThat(read(observation("orders", T.plusSeconds(1), List.of(fact(T, 11))), 1, T).status()).isEqualTo(Status.INVALID_OBSERVATION_TIME);
        Reading inconsistent = read(observation("orders", T, List.of(fact(T.plusSeconds(1), 11))), 1, T.plusSeconds(1));
        assertThat(inconsistent.status()).isEqualTo(Status.INVALID_OBSERVATION_TIME);
        assertThat(inconsistent.signal()).isNull();
    }

    @Test void counterContextIsForwardedWithTheOriginalAccumulationAndPreviousPoint() {
        var metric = new MetricAlertEvaluator.Rule("counter.quantity", MetricType.COUNTER, "1", Map.of(),
                MetricAlertEvaluator.Direction.ABOVE, BigDecimal.TEN, BigDecimal.valueOf(5));
        MetricPoint previous = MetricPoint.accumulated(Map.of(), T.minusSeconds(10), T, 100);
        List<MetricPoint> next = List.of(
                MetricPoint.accumulated(Map.of(), T.minusSeconds(10), T.plusSeconds(5), 200),
                MetricPoint.accumulated(Map.of(), T, T.plusSeconds(5), 200),
                MetricPoint.accumulated(Map.of(), T.minusSeconds(10), T.plusSeconds(11), 200));
        List<MetricAlertEvaluator.Quality> qualities = List.of(MetricAlertEvaluator.Quality.VALID,
                MetricAlertEvaluator.Quality.RESET, MetricAlertEvaluator.Quality.GAP);
        for (int i = 0; i < next.size(); i++) {
            MetricPoint point = next.get(i);
            MetricFact original = MetricFact.single(metric.name(), metric.type(), metric.unit(), point);
            Reading reading = evaluate(RULE, metric,
                    new Publication(RULE, i + 1, observation("orders", point.observedAt(), List.of(original))),
                    BUDGET, previous, false, AlarmSeverity.MAJOR, point.observedAt());
            assertThat(reading.status()).isEqualTo(Status.READY);
            assertThat(reading.point()).isSameAs(point);
            assertThat(reading.evaluation().quality()).isEqualTo(qualities.get(i));
            assertThat(reading.signal().observedAt()).isEqualTo(point.observedAt());
            if (i == 0) {
                assertThat(reading.evaluation().value()).isEqualByComparingTo("20");
                assertThat(reading.signal().condition()).isEqualTo(AlertSignal.Condition.BAD);
            } else {
                assertThat(reading.signal().condition()).isEqualTo(AlertSignal.Condition.UNKNOWN);
            }
        }
    }

    @Test void aFreshPublicationNeverRefreshesAnOldMeasurementAndNoCallbackStillGoesStale() {
        AlertSnapshot state = AlertSnapshot.initial(RULE);
        Reading first = read(observation("orders", T, List.of(fact(T, 11))), 1, T);
        state = AlertLifecycle.evaluate(state, first.signal(), T).snapshot();
        assertThat(state.status()).isEqualTo(AlertSnapshot.Status.PENDING);
        Reading repeated = read(observation("orders", T.plusSeconds(10), List.of(fact(T, 11))), 2, T.plusSeconds(10));
        AlertUpdate duplicate = AlertLifecycle.evaluate(state, repeated.signal(), T.plusSeconds(10));
        assertThat(duplicate.ignored()).isTrue();
        assertThat(duplicate.snapshot()).isEqualTo(state);
        Reading stale = read(observation("orders", T.plusSeconds(16), List.of(fact(T, 11))), 3, T.plusSeconds(16));
        assertThat(stale.evaluation().quality()).isEqualTo(MetricAlertEvaluator.Quality.STALE);
        assertThat(stale.signal().observedAt()).isEqualTo(T);
        AlertSnapshot ticked = AlertLifecycle.tick(state, T.plusSeconds(16)).snapshot();
        assertThat(ticked.status()).isEqualTo(AlertSnapshot.Status.CLEAR);
        assertThat(ticked.quality()).isEqualTo(AlertSnapshot.Quality.STALE);
        assertThat(ticked.episode()).isNull();
    }
}
