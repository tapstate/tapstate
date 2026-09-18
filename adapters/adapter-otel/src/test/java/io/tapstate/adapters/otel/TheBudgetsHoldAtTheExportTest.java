package io.tapstate.adapters.otel;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.PipelineState;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static io.tapstate.adapters.otel.Facts.AT;
import static io.tapstate.adapters.otel.Facts.RECORDS;
import static io.tapstate.adapters.otel.Facts.START;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Both fences on cardinality hold where the facts leave the process, and each is driven past its limit
 * on purpose: a limit never reached is a limit never tested, and a case that stayed under it would pass
 * against an exporter that folded nothing. The counts are printed, not only asserted, so the evidence
 * that the limit was crossed is on the record beside the pass.
 */
class TheBudgetsHoldAtTheExportTest {

    private static final AttributeKey<String> OVERFLOW = AttributeKey.stringKey(MetricAttributes.OVERFLOW);
    private static final AttributeKey<String> DIRECTION = AttributeKey.stringKey(MetricAttributes.DIRECTION);
    private static final AttributeKey<String> TABLE = AttributeKey.stringKey(MetricAttributes.TABLE_ID);

    private final FactsMetricProducer producer = new FactsMetricProducer(START);

    private static Collection<LongPointData> recordsPoints(Collection<MetricData> produced) {
        return produced.stream().filter(metric -> metric.getName().equals(RECORDS)).findFirst()
                .orElseThrow().getLongSumData().getPoints();
    }

    @Test
    void aPipelineWiderThanItsBudgetLeavesWithOneOverflowSeriesPerRemainingCombination() {
        // 1250 tables in each of two directions: 2500 series against a budget of 1000 tables.
        int tables = 1_250;
        List<MetricPoint> points = new ArrayList<>();
        for (int i = 0; i < tables; i++) {
            for (String direction : List.of("in", "out")) {
                points.add(MetricPoint.accumulated(
                        Facts.tableAndDirection("wide", String.format(Locale.ROOT, "t%04d", i), direction), START, AT, 1L));
            }
        }
        producer.offer("wide", PipelineState.RUNNING, AT, List.of(new MetricFact(RECORDS, MetricType.COUNTER, "{record}", points)));

        Collection<LongPointData> exported = recordsPoints(producer.produce(Resource.empty()));
        List<LongPointData> overflow = exported.stream().filter(point -> "true".equals(point.getAttributes().get(OVERFLOW))).toList();
        System.out.printf(Locale.ROOT, "records series offered=%d exported=%d (named=%d, overflow=%d)%n",
                points.size(), exported.size(), exported.size() - overflow.size(), overflow.size());

        assertThat(exported.size()).isLessThan(points.size());
        assertThat(exported).hasSize(2 * CardinalityBudget.RECORDS.distinctValues() + 2);
        // The fold drops the table and keeps the direction: one overflow series per direction, each
        // holding the 250 tables that did not get a name, added up.
        assertThat(overflow).hasSize(2);
        assertThat(overflow).allSatisfy(point -> {
            assertThat(point.getAttributes().get(TABLE)).isNull();
            assertThat(point.getAttributes().get(DIRECTION)).isNotNull();
            assertThat(point.getValue()).isEqualTo(tables - CardinalityBudget.RECORDS.distinctValues());
        });
    }

    @Test
    void pastTheExportLimitAcrossPipelinesTheRestFoldIntoOneOverflowSeriesAndStayFolded() {
        // 5001 pipelines, two series each: 10002 series against the export limit of 10000. The per
        // pipeline fold cannot see this: each pipeline is well within its own budget.
        int pipelines = CardinalityBudget.EXPORT_SERIES_LIMIT / 2 + 1;
        for (int i = 0; i < pipelines; i++) {
            producer.offer(String.format(Locale.ROOT, "p%05d", i), PipelineState.RUNNING, AT,
                    List.of(Facts.records(String.format(Locale.ROOT, "p%05d", i), 1, 1)));
        }

        Collection<LongPointData> exported = recordsPoints(producer.produce(Resource.empty()));
        List<LongPointData> overflow = exported.stream().filter(point -> "true".equals(point.getAttributes().get(OVERFLOW))).toList();
        System.out.printf(Locale.ROOT, "records series offered=%d exported=%d (named=%d, overflow=%d, overflow value=%d)%n",
                pipelines * 2, exported.size(), exported.size() - overflow.size(), overflow.size(),
                overflow.isEmpty() ? -1 : overflow.get(0).getValue());

        assertThat(exported).hasSize(CardinalityBudget.EXPORT_SERIES_LIMIT);
        assertThat(overflow).hasSize(1);
        // The standard overflow series: only the marker, and the sum of everything that did not get a name.
        assertThat(overflow.get(0).getAttributes().asMap()).hasSize(1);
        assertThat(overflow.get(0).getValue()).isEqualTo(pipelines * 2L - (CardinalityBudget.EXPORT_SERIES_LIMIT - 1));

        // First sight decides, and stays decided: the same collection again names the same series.
        Collection<LongPointData> again = recordsPoints(producer.produce(Resource.empty()));
        assertThat(again.stream().map(LongPointData::getAttributes).toList())
                .containsExactlyInAnyOrderElementsOf(exported.stream().map(LongPointData::getAttributes).toList());
    }

    /** Two series (one in, one out) for each of {@code pipelines} pipelines named {@code prefix}NNNNN. */
    private void offerPipelines(String prefix, int pipelines) {
        for (int index = 0; index < pipelines; index++) {
            String id = prefix + String.format(Locale.ROOT, "%05d", index);
            producer.offer(id, PipelineState.RUNNING, AT, List.of(Facts.records(id, 1, 1)));
        }
    }

    @Test
    void aSeriesSeenWhileThereWasRoomKeepsItsNameWhenTheLimitIsCrossedLater() {
        // Under the limit to begin with: 4999 pipelines, two series each, nothing folded anywhere.
        int seenEarly = (CardinalityBudget.EXPORT_SERIES_LIMIT - 2) / 2;
        offerPipelines("a", seenEarly);
        List<Attributes> early = recordsPoints(producer.produce(Resource.empty())).stream()
                .map(LongPointData::getAttributes).toList();
        assertThat(early).hasSize(seenEarly * 2);

        // Then half again as many arrive at once and the total is well past the limit.
        offerPipelines("b", 2_500);
        Collection<LongPointData> exported = recordsPoints(producer.produce(Resource.empty()));
        List<LongPointData> overflow = exported.stream()
                .filter(point -> "true".equals(point.getAttributes().get(OVERFLOW))).toList();
        System.out.printf(Locale.ROOT, "records series seen early=%d offered=%d exported=%d (named=%d, overflow=%d)%n",
                early.size(), seenEarly * 2 + 5_000, exported.size(), exported.size() - overflow.size(),
                overflow.size());

        // Every series that was there while there was room is still there by name. Deciding this at the
        // crossing instead would name whichever 9999 of the 14998 the pipelines currently held happened to
        // iterate first, and the ones offered before the crossing have no claim on that order at all.
        assertThat(exported.stream().map(LongPointData::getAttributes).toList()).containsAll(early);
        assertThat(exported).hasSize(CardinalityBudget.EXPORT_SERIES_LIMIT);
        assertThat(overflow).singleElement().satisfies(series -> assertThat(series.getValue())
                .isEqualTo(seenEarly * 2L + 5_000L - (CardinalityBudget.EXPORT_SERIES_LIMIT - 1)));
    }

    @Test
    void thePerPipelineFoldGivesUpItsValuesWithThePipelineToo() {
        // The fold inside offer() has its own memory of which tables it named, per pipeline. It has to be
        // given up with the pipeline as well: an id applied again over other tables would otherwise find
        // its budget spent on the tables of the pipeline that had the id before.
        int tables = CardinalityBudget.RECORDS.distinctValues() + 200;
        producer.offer("orders", PipelineState.RUNNING, AT, List.of(rowsOver("orders", tables, "t")));
        assertThat(namedTables(recordsPoints(producer.produce(Resource.empty()))))
                .hasSize(CardinalityBudget.RECORDS.distinctValues());
        producer.forgetPipelinesOutside(List.of());

        producer.offer("orders", PipelineState.RUNNING, AT, List.of(rowsOver("orders", tables, "u")));

        assertThat(namedTables(recordsPoints(producer.produce(Resource.empty()))))
                .hasSize(CardinalityBudget.RECORDS.distinctValues())
                .allSatisfy(table -> assertThat(table).startsWith("u"));
    }

    /** One records fact over {@code tables} tables of {@code pipelineId}, in and out, each table named by the prefix. */
    private static MetricFact rowsOver(String pipelineId, int tables, String prefix) {
        List<MetricPoint> points = new ArrayList<>();
        for (int index = 0; index < tables; index++) {
            String table = prefix + String.format(Locale.ROOT, "%04d", index);
            for (String direction : List.of("in", "out")) {
                points.add(MetricPoint.accumulated(
                        Facts.tableAndDirection(pipelineId, table, direction), START, AT, 1L));
            }
        }
        return new MetricFact(RECORDS, MetricType.COUNTER, "{record}", points);
    }

    /** The distinct table ids the export named, overflow series excluded. */
    private static List<String> namedTables(Collection<LongPointData> exported) {
        return exported.stream().map(point -> point.getAttributes().get(TABLE))
                .filter(table -> table != null).distinct().toList();
    }

    @Test
    void aNameIsGivenUpWithThePipelineItWasHeldFor() {
        // The limit nearly filled by pipelines that then go away.
        int wide = (CardinalityBudget.EXPORT_SERIES_LIMIT - 2) / 2;
        offerPipelines("gone", wide);
        assertThat(recordsPoints(producer.produce(Resource.empty()))).hasSize(wide * 2);
        producer.forgetPipelinesOutside(List.of());

        // A fresh set, over the limit on its own: what it may name has to be the whole limit again. Held
        // names would leave it one, and everything a running deployment exports would arrive as overflow
        // because of pipelines that were removed.
        offerPipelines("live", 6_000);
        Collection<LongPointData> exported = recordsPoints(producer.produce(Resource.empty()));
        List<LongPointData> overflow = exported.stream()
                .filter(point -> "true".equals(point.getAttributes().get(OVERFLOW))).toList();
        System.out.printf(Locale.ROOT, "after forgetting %d pipelines: offered=%d exported=%d (named=%d)%n",
                wide, 12_000, exported.size(), exported.size() - overflow.size());

        assertThat(exported).hasSize(CardinalityBudget.EXPORT_SERIES_LIMIT);
        assertThat(overflow).hasSize(1);
        assertThat(exported.size() - overflow.size()).isEqualTo(CardinalityBudget.EXPORT_SERIES_LIMIT - 1);
    }
}
