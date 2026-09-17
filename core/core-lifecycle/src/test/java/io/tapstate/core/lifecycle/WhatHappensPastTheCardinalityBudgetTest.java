package io.tapstate.core.lifecycle;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.tapstate.core.lifecycle.MetricAttributes.CODE;
import static io.tapstate.core.lifecycle.MetricAttributes.DIRECTION;
import static io.tapstate.core.lifecycle.MetricAttributes.OP;
import static io.tapstate.core.lifecycle.MetricAttributes.OVERFLOW;
import static io.tapstate.core.lifecycle.MetricAttributes.PIPELINE_ID;
import static io.tapstate.core.lifecycle.MetricAttributes.STAGE;
import static io.tapstate.core.lifecycle.MetricAttributes.TABLE_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What becomes of a metric's series past the number its instrument budgeted. The budget table itself is a
 * copy of the design document's on purpose, so that a number changed on one side without the other
 * reddens here; the rest builds inputs that really are past the budget — the count is printed, because a
 * limit a test never reaches is a limit that has never once been exercised — and checks that what happens
 * to them is the one thing the document says happens.
 */
class WhatHappensPastTheCardinalityBudgetTest {

    private static final Instant STARTED = Instant.parse("2026-09-17T00:00:00Z");
    private static final Instant OBSERVED = Instant.parse("2026-09-17T00:05:00Z");
    private static final Instant LATER = Instant.parse("2026-09-17T00:06:00Z");
    private static final String PIPELINE = "orders_sync";

    private static String table(int index) {
        return String.format("t%04d", index);
    }

    /** Rows of {@code tables} tables, in and out, each table's count being its index. */
    private static MetricFact rowsOver(int tables, String pipeline) {
        return rowsOver(tables, pipeline, "t");
    }

    /** The same, with every table's name starting with {@code prefix}, for two pipelines over different tables. */
    private static MetricFact rowsOver(int tables, String pipeline, String prefix) {
        List<MetricPoint> points = new ArrayList<>();
        for (int index = 1; index <= tables; index++) {
            for (String direction : List.of("in", "out")) {
                points.add(MetricPoint.accumulated(Map.of(PIPELINE_ID, pipeline, TABLE_ID,
                        prefix + String.format("%04d", index), DIRECTION, direction, OP, "insert"),
                        STARTED, OBSERVED, index));
            }
        }
        return new MetricFact("tapstate.pipeline.records", MetricType.COUNTER, "{record}", points);
    }

    private static long total(MetricFact fact, String direction) {
        return fact.points().stream()
                .filter(point -> direction.equals(point.attributes().get(DIRECTION)))
                .mapToLong(MetricPoint::value).sum();
    }

    private static List<MetricPoint> overflowSeries(MetricFact fact) {
        return fact.points().stream().filter(point -> point.attributes().containsKey(OVERFLOW)).toList();
    }

    private static List<String> namedTables(MetricFact fact) {
        return fact.points().stream().map(point -> point.attributes().get(TABLE_ID))
                .filter(table -> table != null).distinct().toList();
    }

    @Test
    @DisplayName("every instrument declares a budget on its one growing dimension, and nothing else does")
    void theBudgetTableIsTheDocumentedOne() {
        assertThat(Arrays.stream(CardinalityBudget.values()).map(CardinalityBudget::instrument))
                .containsExactlyInAnyOrder(
                        "tapstate.pipeline.records",
                        "tapstate.pipeline.bytes",
                        "tapstate.pipeline.lag",
                        "tapstate.pipeline.record.delivery.duration",
                        "tapstate.pipeline.process.duration",
                        "tapstate.pipeline.snapshot.rows",
                        "tapstate.pipeline.snapshot.rows.total",
                        "tapstate.pipeline.errors",
                        "tapstate.pipeline.frontier.gap",
                        "tapstate.pipeline.frontier.stall",
                        "tapstate.pipeline.nest.entries",
                        "tapstate.pipeline.nest.accesses",
                        "tapstate.pipeline.nest.backfills",
                        "tapstate.pipeline.nest.backfill.time",
                        "tapstate.pipeline.nest.pending.high_water",
                        "tapstate.pipeline.nest.stored",
                        "tapstate.pipeline.nest.dead_lettered",
                        "tapstate.pipeline.join.recompute.rows",
                        "tapstate.pipeline.join.recompute.rows.total",
                        "tapstate.pipeline.records.driven",
                        "tapstate.pipeline.reconcile.failures.streak");
        for (CardinalityBudget budget : List.of(CardinalityBudget.RECORDS, CardinalityBudget.BYTES,
                CardinalityBudget.LAG, CardinalityBudget.RECORD_DELIVERY_DURATION,
                CardinalityBudget.SNAPSHOT_ROWS, CardinalityBudget.SNAPSHOT_ROWS_TOTAL)) {
            assertThat(budget.openDimension()).as(budget.name()).contains(TABLE_ID);
            assertThat(budget.distinctValues()).as(budget.name()).isEqualTo(1_000);
        }
        // The families a definition draws are budgeted like the table: the definition names them, rows cannot.
        for (CardinalityBudget budget : List.of(CardinalityBudget.FRONTIER_GAP, CardinalityBudget.FRONTIER_STALL)) {
            assertThat(budget.openDimension()).as(budget.name()).contains(MetricAttributes.CHAIN_ID);
            assertThat(budget.distinctValues()).as(budget.name()).isEqualTo(1_000);
        }
        for (CardinalityBudget budget : List.of(CardinalityBudget.NEST_ENTRIES, CardinalityBudget.NEST_ACCESSES,
                CardinalityBudget.NEST_BACKFILLS, CardinalityBudget.NEST_BACKFILL_TIME,
                CardinalityBudget.NEST_PENDING_HIGH_WATER, CardinalityBudget.NEST_STORED,
                CardinalityBudget.NEST_DEAD_LETTERED)) {
            assertThat(budget.openDimension()).as(budget.name()).contains(MetricAttributes.NEST_NAMESPACE);
            assertThat(budget.distinctValues()).as(budget.name()).isEqualTo(1_000);
        }
        for (CardinalityBudget budget : List.of(CardinalityBudget.JOIN_RECOMPUTE_ROWS,
                CardinalityBudget.JOIN_RECOMPUTE_ROWS_TOTAL)) {
            assertThat(budget.openDimension()).as(budget.name()).contains(MetricAttributes.JOIN_NAMESPACE);
            assertThat(budget.distinctValues()).as(budget.name()).isEqualTo(1_000);
        }
        assertThat(CardinalityBudget.ERRORS.openDimension()).contains(CODE);
        assertThat(CardinalityBudget.ERRORS.distinctValues()).isEqualTo(200);
        assertThat(CardinalityBudget.PROCESS_DURATION.openDimension()).isEmpty();
        assertThat(CardinalityBudget.PROCESS_DURATION.distinctValues()).isEqualTo(Stage.values().length);
        for (CardinalityBudget budget : List.of(CardinalityBudget.RECORDS_DRIVEN,
                CardinalityBudget.RECONCILE_FAILURES_STREAK)) {
            assertThat(budget.openDimension()).as(budget.name()).isEmpty();
            assertThat(budget.distinctValues()).as(budget.name()).isEqualTo(1);
        }
        assertThat(CardinalityBudget.EXPORT_SERIES_LIMIT).isEqualTo(10_000);
        // The flat spellings are keys of a face, not instruments: none of them declares a budget.
        assertThat(CardinalityBudget.forInstrument("reconcileFailuresInARow")).isEmpty();
        assertThat(CardinalityBudget.forInstrument("recordCount")).isEmpty();
    }

    @Test
    @DisplayName("the export backstop holds one pipeline at its full budget whole")
    void theExportBackstopHoldsAFullPipelineWhole() {
        // The widest instrument: a thousand tables, two directions, and the five operations an engine
        // produces (the sixth, "other", is for what it does not recognise).
        assertThat(CardinalityBudget.EXPORT_SERIES_LIMIT)
                .isGreaterThanOrEqualTo(CardinalityBudget.RECORDS.distinctValues() * 2 * 5);
    }

    @Test
    @DisplayName("a metric that carries attributes and declares no budget is refused where it is built")
    void anUnbudgetedDimensionedMetricIsRefused() {
        assertThatThrownBy(() -> MetricFact.single("tapstate.pipeline.novel", MetricType.GAUGE, "1",
                MetricPoint.reading(Map.of(TABLE_ID, "orders"), OBSERVED, 1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cardinality budget");
        // The same name with nothing to break out is one series, and one series needs no budget.
        assertThat(MetricFact.single("tapstate.pipeline.novel", MetricType.GAUGE, "1",
                MetricPoint.reading(Map.of(), OBSERVED, 1L)).points()).hasSize(1);
    }

    @Test
    @DisplayName("past the budget the excess tables fold into one series per direction, and the total survives")
    void theExcessFoldsAndTheTotalSurvives() {
        MetricFact wide = rowsOver(1_250, PIPELINE);

        MetricFact folded = CardinalityBudget.folder().fold(wide);

        System.out.printf("cardinality: %d points over %d tables in, %d points out (%d named tables, %d"
                        + " overflow series)%n", wide.points().size(), namedTables(wide).size(),
                folded.points().size(), namedTables(folded).size(), overflowSeries(folded).size());
        assertThat(wide.points()).hasSize(2_500);
        assertThat(folded.points()).hasSize(2_002);
        assertThat(namedTables(folded)).hasSize(1_000);
        assertThat(overflowSeries(folded)).hasSize(2).allSatisfy(series -> {
            assertThat(series.attributes()).containsEntry(PIPELINE_ID, PIPELINE)
                    .containsEntry(OP, "insert").containsEntry(OVERFLOW, "true")
                    .doesNotContainKey(TABLE_ID);
            assertThat(series.startTime()).isEqualTo(STARTED);
            // Tables 1001..1250 by index: (1001 + 1250) * 250 / 2
            assertThat(series.value()).isEqualTo(281_375L);
        });
        for (String direction : List.of("in", "out")) {
            assertThat(total(folded, direction)).as(direction).isEqualTo(total(wide, direction));
        }
    }

    @Test
    @DisplayName("which tables keep a series of their own is decided by first sight, not by this tick's order")
    void whichSeriesAreNamedIsDecidedByFirstSight() {
        CardinalityBudget.Folder folder = CardinalityBudget.folder();
        MetricFact firstTick = rowsOver(1_000, PIPELINE);
        assertThat(folder.fold(firstTick)).isSameAs(firstTick);

        // A table whose name sorts before every named one, arriving once the budget is full.
        List<MetricPoint> next = new ArrayList<>();
        next.add(MetricPoint.accumulated(Map.of(PIPELINE_ID, PIPELINE, TABLE_ID, "aaaa", DIRECTION, "in",
                OP, "insert"), STARTED, LATER, 5L));
        next.addAll(rowsOver(1_000, PIPELINE).points());
        MetricFact folded = folder.fold(new MetricFact("tapstate.pipeline.records", MetricType.COUNTER,
                "{record}", next));

        assertThat(namedTables(folded)).hasSize(1_000).doesNotContain("aaaa").contains(table(1));
        assertThat(overflowSeries(folded)).singleElement().satisfies(series -> {
            assertThat(series.attributes()).containsEntry(DIRECTION, "in");
            assertThat(series.value()).isEqualTo(5L);
        });
    }

    @Test
    @DisplayName("the budget is per pipeline, so one wide pipeline cannot crowd another out")
    void theBudgetIsPerPipeline() {
        CardinalityBudget.Folder folder = CardinalityBudget.folder();
        // Two pipelines over two disjoint sets of tables: a budget kept per process would have the
        // second pipeline's every table arrive to a set the first had already filled.
        MetricFact first = rowsOver(1_000, "a", "t");
        MetricFact second = rowsOver(1_000, "b", "u");

        assertThat(folder.fold(first)).isSameAs(first);
        assertThat(folder.fold(second)).isSameAs(second);
    }

    @Test
    @DisplayName("a gauge past its budget keeps the highest of the folded readings")
    void aFoldedGaugeKeepsTheHighestReading() {
        List<MetricPoint> points = new ArrayList<>();
        for (int index = 1; index <= 1_003; index++) {
            points.add(MetricPoint.reading(Map.of(PIPELINE_ID, PIPELINE, TABLE_ID, table(index)), OBSERVED,
                    index));
        }
        MetricFact folded = CardinalityBudget.folder().fold(
                new MetricFact("tapstate.pipeline.lag", MetricType.GAUGE, "s", points));

        assertThat(folded.points()).hasSize(1_001);
        assertThat(overflowSeries(folded)).singleElement()
                .satisfies(series -> assertThat(series.value()).isEqualTo(1_003L));
    }

    @Test
    @DisplayName("a histogram past its budget adds its buckets together")
    void aFoldedHistogramAddsItsBuckets() {
        HistogramBounds bounds = HistogramBounds.RECORD_DELIVERY_DURATION;
        List<MetricPoint> points = new ArrayList<>();
        for (int index = 1; index <= 1_002; index++) {
            List<Long> counts = new ArrayList<>(Collections.nCopies(bounds.buckets(), 0L));
            counts.set(index % bounds.buckets(), 1L);
            points.add(MetricPoint.distribution(Map.of(PIPELINE_ID, PIPELINE, TABLE_ID, table(index)),
                    STARTED, OBSERVED, bounds.value(1, 0.5, counts)));
        }
        MetricFact folded = CardinalityBudget.folder().fold(new MetricFact(
                "tapstate.pipeline.record.delivery.duration", MetricType.HISTOGRAM, "s", points));

        assertThat(folded.points()).hasSize(1_001);
        assertThat(overflowSeries(folded)).singleElement().satisfies(series -> {
            HistogramValue merged = series.histogram();
            assertThat(merged.count()).isEqualTo(2L);
            assertThat(merged.sum()).isEqualTo(1.0);
            assertThat(merged.bounds()).isEqualTo(bounds.bounds());
            // Tables 1001 and 1002 land in buckets 1001 % 16 = 9 and 1002 % 16 = 10.
            assertThat(merged.bucketCounts().get(9)).isEqualTo(1L);
            assertThat(merged.bucketCounts().get(10)).isEqualTo(1L);
            assertThat(merged.bucketCounts().stream().mapToLong(Long::longValue).sum()).isEqualTo(2L);
        });
    }

    @Test
    @DisplayName("failures fold on their code")
    void failuresFoldOnTheirCode() {
        List<MetricPoint> points = new ArrayList<>();
        for (int index = 1; index <= 201; index++) {
            points.add(MetricPoint.accumulated(Map.of(PIPELINE_ID, PIPELINE, CODE, "sink.code-" + index),
                    STARTED, OBSERVED, index));
        }
        MetricFact folded = CardinalityBudget.folder().fold(
                new MetricFact("tapstate.pipeline.errors", MetricType.COUNTER, "{error}", points));

        assertThat(folded.points()).hasSize(201);
        assertThat(overflowSeries(folded)).singleElement().satisfies(series -> {
            assertThat(series.attributes()).containsOnlyKeys(PIPELINE_ID, OVERFLOW);
            assertThat(series.value()).isEqualTo(201L);
        });
    }

    @Test
    @DisplayName("an instrument whose dimensions are all closed never folds")
    void aClosedInstrumentNeverFolds() {
        List<Long> counts = new ArrayList<>(Collections.nCopies(HistogramBounds.PROCESS_DURATION.buckets(), 0L));
        counts.set(3, 1L);
        List<MetricPoint> points = Arrays.stream(Stage.values())
                .map(stage -> MetricPoint.distribution(
                        Map.of(PIPELINE_ID, PIPELINE, STAGE, stage.attributeValue()), STARTED, OBSERVED,
                        HistogramBounds.PROCESS_DURATION.value(1, 0.001, counts)))
                .collect(Collectors.toList());
        MetricFact spent = new MetricFact("tapstate.pipeline.process.duration", MetricType.HISTOGRAM, "s",
                points);

        assertThat(CardinalityBudget.folder().fold(spent)).isSameAs(spent);
    }

    @Test
    @DisplayName("a series already folded upstream joins the fold rather than clashing with it")
    void anUpstreamOverflowSeriesJoinsTheFold() {
        List<MetricPoint> points = new ArrayList<>(rowsOver(1_001, PIPELINE).points());
        points.add(MetricPoint.accumulated(Map.of(PIPELINE_ID, PIPELINE, DIRECTION, "in", OP, "insert",
                OVERFLOW, "true"), STARTED, OBSERVED, 5L));
        MetricFact folded = CardinalityBudget.folder().fold(
                new MetricFact("tapstate.pipeline.records", MetricType.COUNTER, "{record}", points));

        assertThat(overflowSeries(folded)).hasSize(2);
        assertThat(overflowSeries(folded).stream()
                .filter(series -> "in".equals(series.attributes().get(DIRECTION)))
                .mapToLong(MetricPoint::value).sum()).isEqualTo(1_001L + 5L);
    }

    @Test
    @DisplayName("an accumulation that folds several begins when the earliest of them began")
    void aFoldedAccumulationBeginsWhenTheEarliestBegan() {
        Instant earlier = Instant.parse("2026-09-16T00:00:00Z");
        List<MetricPoint> points = new ArrayList<>();
        for (int index = 1; index <= 1_002; index++) {
            // The last table's accumulation began a day before every other's.
            Instant began = index == 1_002 ? earlier : STARTED;
            points.add(MetricPoint.accumulated(Map.of(PIPELINE_ID, PIPELINE, TABLE_ID, table(index),
                    DIRECTION, "in", OP, "insert"), began, OBSERVED, index));
        }
        MetricFact folded = CardinalityBudget.folder().fold(
                new MetricFact("tapstate.pipeline.records", MetricType.COUNTER, "{record}", points));

        assertThat(overflowSeries(folded)).singleElement().satisfies(series -> {
            assertThat(series.startTime()).isEqualTo(earlier);
            assertThat(series.value()).isEqualTo(1_001L + 1_002L);
        });
    }

    @Test
    @DisplayName("a metric with no budget, or with nothing past it, comes back as it went in")
    void nothingToFoldIsTheSameFact() {
        MetricFact plain = MetricFact.single("reconcileFailuresInARow", MetricType.GAUGE, "{pass}",
                MetricPoint.reading(Map.of(), OBSERVED, 3L));
        MetricFact narrow = rowsOver(12, PIPELINE);
        CardinalityBudget.Folder folder = CardinalityBudget.folder();

        assertThat(folder.fold(plain)).isSameAs(plain);
        assertThat(folder.fold(narrow)).isSameAs(narrow);
    }
}
