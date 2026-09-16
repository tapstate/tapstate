package io.tapstate.core.lifecycle;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the flat view does with a metric it cannot carry whole but is asked to carry anyway. A caller may
 * hand it a rule that squeezes a dimensioned metric onto one key per surviving dimension — the throughput
 * of a whole pipeline rather than of each table, each direction and each operation separately — and the
 * question these cover is whether the squeeze is ever silent.
 *
 * <p>It must not be. A collapsed metric reads like any other number on this face, and a reader who thinks
 * they are looking at one table's rows when they are looking at every table's is worse off than a reader
 * who finds the metric missing: the missing one sends them to look elsewhere, and the collapsed one does
 * not. So a reduction is recorded as having happened, beside the drops, and the two are counted apart
 * because they call for different things — a drop wants a new face, a reduction wants the reader told what
 * they are reading.
 */
class AReducedViewNeverCollapsesQuietlyTest {

    private static final Instant STARTED = Instant.parse("2026-09-16T00:00:00Z");
    private static final Instant OBSERVED = Instant.parse("2026-09-16T00:05:00Z");

    /** Rows crossing a boundary, broken out the way the engine really reports them. */
    private static MetricFact records(long ordersIn, long ordersOut, long itemsIn) {
        return new MetricFact("tapstate.pipeline.records", MetricType.COUNTER, "{record}", List.of(
                MetricPoint.accumulated(
                        Map.of("tapstate.table.id", "orders", "direction", "in", "op", "insert"),
                        STARTED, OBSERVED, ordersIn),
                MetricPoint.accumulated(
                        Map.of("tapstate.table.id", "orders", "direction", "out", "op", "insert"),
                        STARTED, OBSERVED, ordersOut),
                MetricPoint.accumulated(
                        Map.of("tapstate.table.id", "items", "direction", "in", "op", "update"),
                        STARTED, OBSERVED, itemsIn)));
    }

    @Test
    @DisplayName("a counter collapsed onto one key per direction sums the points that share it")
    void aCollapsedCounterAddsUpTheRowsBehindEachKey() {
        FlatMetricProjection projected = FlatMetricProjection.of(
                List.of(records(1200L, 1180L, 40L)),
                Map.of("tapstate.pipeline.records",
                        attributes -> "records." + attributes.get("direction")));

        // orders-in and items-in are two points of one key and the work they measure adds up; a reader
        // asking how much this pipeline took in is asking for their sum, not for whichever arrived last.
        assertThat(projected.metrics())
                .containsExactlyInAnyOrderEntriesOf(Map.of("records.in", 1240L, "records.out", 1180L));
    }

    @Test
    @DisplayName("a collapsed metric is named as reduced, so the loss of its dimensions is on the record")
    void aCollapsedMetricSaysItWasCollapsed() {
        FlatMetricProjection projected = FlatMetricProjection.of(
                List.of(records(1200L, 1180L, 40L)),
                Map.of("tapstate.pipeline.records",
                        attributes -> "records." + attributes.get("direction")));

        assertThat(projected.reduced()).containsExactly("tapstate.pipeline.records");
        // Not also dropped: it reached the face. The two lists answer different questions, and a metric
        // in both would say it did and did not arrive.
        assertThat(projected.dropped()).isEmpty();
    }

    @Test
    @DisplayName("a gauge collapsed onto one key keeps the worst reading, because readings do not add up")
    void aCollapsedGaugeKeepsTheWorstOfTheReadings() {
        FlatMetricProjection projected = FlatMetricProjection.of(
                List.of(new MetricFact("tapstate.pipeline.lag", MetricType.GAUGE, "s", List.of(
                        MetricPoint.reading(Map.of("tapstate.table.id", "orders"), OBSERVED, 2L),
                        MetricPoint.reading(Map.of("tapstate.table.id", "items"), OBSERVED, 47L)))),
                Map.of("tapstate.pipeline.lag", attributes -> "lag"));

        // 49 seconds behind is a number no table is, and one nobody could act on. A pipeline is as far
        // behind as its worst table, which is the same rule the engine already applies to two sinks over
        // one chain.
        assertThat(projected.metrics()).containsExactlyInAnyOrderEntriesOf(Map.of("lag", 47L));
        assertThat(projected.reduced()).containsExactly("tapstate.pipeline.lag");
    }

    @Test
    @DisplayName("a rule that keeps a dimension writes one key per value of it, and nothing is summed")
    void aRuleThatKeepsADimensionLeavesItsPointsApart() {
        FlatMetricProjection projected = FlatMetricProjection.of(
                List.of(new MetricFact("tapstate.pipeline.lag", MetricType.GAUGE, "s", List.of(
                        MetricPoint.reading(Map.of("tapstate.table.id", "orders"), OBSERVED, 2L),
                        MetricPoint.reading(Map.of("tapstate.table.id", "items"), OBSERVED, 47L)))),
                Map.of("tapstate.pipeline.lag",
                        attributes -> "lag." + attributes.get("tapstate.table.id")));

        assertThat(projected.metrics())
                .containsExactlyInAnyOrderEntriesOf(Map.of("lag.orders", 2L, "lag.items", 47L));
        // Still reduced: what it lost is not a point but the attributes themselves, which is exactly the
        // loss the eleven families already on this face took, and the one nobody wrote down at the time.
        assertThat(projected.reduced()).containsExactly("tapstate.pipeline.lag");
    }

    @Test
    @DisplayName("a metric with no rule is dropped as before, rules for others notwithstanding")
    void anUnruledMetricIsStillDropped() {
        FlatMetricProjection projected = FlatMetricProjection.of(
                List.of(records(1200L, 1180L, 40L),
                        MetricFact.single("tapstate.pipeline.bytes", MetricType.COUNTER, "By",
                                MetricPoint.accumulated(Map.of("direction", "in"), STARTED, OBSERVED,
                                        512L))),
                Map.of("tapstate.pipeline.records",
                        attributes -> "records." + attributes.get("direction")));

        assertThat(projected.metrics()).containsOnlyKeys("records.in", "records.out");
        assertThat(projected.dropped()).containsExactly("tapstate.pipeline.bytes");
        assertThat(projected.reduced()).containsExactly("tapstate.pipeline.records");
    }

    @Test
    @DisplayName("a distribution is dropped even with a rule, because no key makes it one number")
    void aRuleCannotRescueAHistogram() {
        FlatMetricProjection projected = FlatMetricProjection.of(
                List.of(MetricFact.single("tapstate.pipeline.process.duration", MetricType.HISTOGRAM, "s",
                        MetricPoint.distribution(Map.of("stage", "write"), STARTED, OBSERVED,
                                new HistogramValue(3, 1.5, List.of(1.0), List.of(2L, 1L))))),
                Map.of("tapstate.pipeline.process.duration", attributes -> "processDuration"));

        assertThat(projected.metrics()).isEmpty();
        assertThat(projected.dropped()).containsExactly("tapstate.pipeline.process.duration");
        assertThat(projected.reduced()).isEmpty();
    }

    @Test
    @DisplayName("an undimensioned metric is carried whole, not reduced, even when a rule names it")
    void aMetricThatFitsIsNotCollapsedJustBecauseARuleExists() {
        FlatMetricProjection projected = FlatMetricProjection.of(
                List.of(MetricFact.single("errorCount", MetricType.GAUGE, "{error}",
                        MetricPoint.reading(Map.of(), OBSERVED, 0L))),
                Map.of("errorCount", attributes -> "somethingElse"));

        // The rule is for a metric this face cannot hold. Applying it to one it can would rename a key
        // five tests and a published document already read, and would report a reduction that never
        // happened.
        assertThat(projected.metrics()).containsExactlyInAnyOrderEntriesOf(Map.of("errorCount", 0L));
        assertThat(projected.reduced()).isEmpty();
    }

    @Test
    @DisplayName("a rule with no key for a point is refused rather than quietly losing it")
    void aRuleThatAnswersNothingIsRefused() {
        List<MetricFact> facts = List.of(records(1200L, 1180L, 40L));
        Map<String, FlatReduction> rules = Map.of("tapstate.pipeline.records",
                attributes -> "out".equals(attributes.get("direction")) ? "records.out" : null);

        // Silently skipping the point would put a number on this face that is short by every point the
        // rule had no answer for, and it would look exactly like a correct one. This class exists to stop
        // that, so it cannot be the thing that does it.
        assertThatThrownBy(() -> FlatMetricProjection.of(facts, rules))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tapstate.pipeline.records");
    }

    @Test
    @DisplayName("the old one-argument projection reduces nothing at all")
    void withoutRulesNothingIsReduced() {
        FlatMetricProjection projected = FlatMetricProjection.of(List.of(records(1200L, 1180L, 40L)));

        assertThat(projected.metrics()).isEmpty();
        assertThat(projected.dropped()).containsExactly("tapstate.pipeline.records");
        assertThat(projected.reduced()).isEmpty();
    }
}
