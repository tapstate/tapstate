package io.tapstate.adapters.otel;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.data.SumData;
import io.opentelemetry.sdk.resources.Resource;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.PipelineState;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.tapstate.adapters.otel.Facts.AT;
import static io.tapstate.adapters.otel.Facts.DELIVERY;
import static io.tapstate.adapters.otel.Facts.LAG;
import static io.tapstate.adapters.otel.Facts.RECORDS;
import static io.tapstate.adapters.otel.Facts.START;
import static io.tapstate.adapters.otel.Facts.nanos;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The facts are projected onto the SDK's data model as they are: the same names, attributes, units,
 * times and numbers, in the type each kind of fact is.
 */
class FactsBecomeMetricDataTest {

    private final FactsMetricProducer producer = new FactsMetricProducer(START.minusSeconds(5));

    @Test
    void processHealthIsPulledWithoutAnyPipelineObservationOrStoreWrite() {
        AtomicLong degraded = new AtomicLong(1);
        producer.observeProcess(() -> List.of(MetricFact.single(
                "tapstate.process.telemetry.degraded", MetricType.GAUGE, "1",
                MetricPoint.reading(Map.of(MetricAttributes.TELEMETRY_SINK, "latest"), AT,
                        degraded.get()))));

        MetricData first = only(producer.produce(Resource.empty()),
                "tapstate.process.telemetry.degraded");
        assertThat(first.getType()).isEqualTo(MetricDataType.LONG_GAUGE);
        LongPointData point = first.getLongGaugeData().getPoints().iterator().next();
        assertThat(point.getValue()).isEqualTo(1L);
        assertThat(point.getAttributes().get(AttributeKey.stringKey(MetricAttributes.TELEMETRY_SINK)))
                .isEqualTo("latest");
        assertThat(point.getAttributes().get(AttributeKey.stringKey(MetricAttributes.PIPELINE_ID))).isNull();

        degraded.set(0);
        assertThat(only(producer.produce(Resource.empty()), "tapstate.process.telemetry.degraded")
                .getLongGaugeData().getPoints()).extracting(LongPointData::getValue).containsExactly(0L);
    }

    private static MetricData only(Collection<MetricData> produced, String name) {
        List<MetricData> named = produced.stream().filter(metric -> metric.getName().equals(name)).toList();
        assertThat(named).as("one metric named %s", name).hasSize(1);
        return named.get(0);
    }

    private static <T extends PointData> T with(Collection<T> points, String attribute, String value) {
        return points.stream()
                .filter(point -> value.equals(point.getAttributes().get(AttributeKey.stringKey(attribute))))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no point with " + attribute + "=" + value + " in " + points));
    }

    @Test
    void aCounterFactIsACumulativeMonotonicSumWithItsOwnStartAndAttributes() {
        producer.offer("orders_sync", PipelineState.RUNNING, AT, List.of(Facts.records("orders_sync", 42, 50)));

        MetricData records = only(producer.produce(Resource.empty()), RECORDS);

        assertThat(records.getType()).isEqualTo(MetricDataType.LONG_SUM);
        assertThat(records.getUnit()).isEqualTo("{record}");
        assertThat(records.getInstrumentationScopeInfo().getName()).isEqualTo("io.tapstate");
        SumData<LongPointData> sum = records.getLongSumData();
        assertThat(sum.isMonotonic()).isTrue();
        assertThat(sum.getAggregationTemporality()).isEqualTo(AggregationTemporality.CUMULATIVE);
        assertThat(sum.getPoints()).extracting(LongPointData::getValue).containsExactlyInAnyOrder(42L, 50L);
        LongPointData out = with(sum.getPoints(), MetricAttributes.DIRECTION, "out");
        assertThat(out.getValue()).isEqualTo(42L);
        assertThat(out.getStartEpochNanos()).isEqualTo(nanos(START));
        assertThat(out.getEpochNanos()).isEqualTo(nanos(AT));
        assertThat(out.getAttributes().get(AttributeKey.stringKey(MetricAttributes.TABLE_ID))).isEqualTo("orders");
        assertThat(out.getAttributes().get(AttributeKey.stringKey(MetricAttributes.PIPELINE_ID))).isEqualTo("orders_sync");
    }

    @Test
    void aGaugeFactIsAGaugeDatedFromWhenTheExporterCameUp() {
        producer.offer("orders_sync", PipelineState.RUNNING, AT, List.of(Facts.lag("orders_sync", 7)));

        MetricData lag = only(producer.produce(Resource.empty()), LAG);

        assertThat(lag.getType()).isEqualTo(MetricDataType.LONG_GAUGE);
        assertThat(lag.getUnit()).isEqualTo("s");
        LongPointData point = lag.getLongGaugeData().getPoints().iterator().next();
        assertThat(point.getValue()).isEqualTo(7L);
        assertThat(point.getEpochNanos()).isEqualTo(nanos(AT));
        // A reading has no start of its own; what stands in is when this exporter came up, never nought.
        assertThat(point.getStartEpochNanos()).isEqualTo(nanos(START.minusSeconds(5)));
    }

    @Test
    void aHistogramFactKeepsItsOwnBucketsCountAndSum() {
        producer.offer("orders_sync", PipelineState.RUNNING, AT, List.of(Facts.delivery("orders_sync")));

        MetricData delivery = only(producer.produce(Resource.empty()), DELIVERY);

        assertThat(delivery.getType()).isEqualTo(MetricDataType.HISTOGRAM);
        assertThat(delivery.getHistogramData().getAggregationTemporality()).isEqualTo(AggregationTemporality.CUMULATIVE);
        HistogramPointData point = delivery.getHistogramData().getPoints().iterator().next();
        assertThat(point.getCount()).isEqualTo(3L);
        assertThat(point.getSum()).isEqualTo(1.5);
        assertThat(point.getBoundaries()).containsExactlyElementsOf(Facts.DELIVERY_BOUNDS);
        assertThat(point.getCounts()).containsExactlyElementsOf(Facts.deliveryCounts());
        assertThat(point.getStartEpochNanos()).isEqualTo(nanos(START));
        assertThat(point.hasMin()).isFalse();
        assertThat(point.hasMax()).isFalse();
    }

    @Test
    void theStateIsAGaugePerStateOneAtOneAndTheRestAtNought() {
        producer.offer("orders_sync", PipelineState.PAUSED, AT, List.of(Facts.lag("orders_sync", 1)));

        MetricData state = only(producer.produce(Resource.empty()), FactsMetricProducer.STATE_METRIC);

        assertThat(state.getType()).isEqualTo(MetricDataType.LONG_GAUGE);
        Collection<LongPointData> points = state.getLongGaugeData().getPoints();
        assertThat(points).hasSize(PipelineState.values().length);
        assertThat(with(points, FactsMetricProducer.STATE_ATTRIBUTE, "paused").getValue()).isEqualTo(1L);
        assertThat(points.stream().mapToLong(LongPointData::getValue).sum())
                .as("exactly one state is the state the pipeline is in").isEqualTo(1L);
        assertThat(with(points, FactsMetricProducer.STATE_ATTRIBUTE, "running").getValue()).isZero();
    }

    @Test
    void aLaterOfferReplacesThePipelinesFactsAndAForgottenPipelinesSeriesStop() {
        producer.offer("a", PipelineState.RUNNING, AT, List.of(Facts.records("a", 1, 1)));
        producer.offer("b", PipelineState.RUNNING, AT, List.of(Facts.records("b", 2, 2)));
        producer.offer("a", PipelineState.RUNNING, AT.plusSeconds(1), List.of(Facts.records("a", 10, 10)));

        SumData<LongPointData> both = only(producer.produce(Resource.empty()), RECORDS).getLongSumData();
        assertThat(both.getPoints()).hasSize(4);
        assertThat(with(both.getPoints(), MetricAttributes.PIPELINE_ID, "a").getValue()).isEqualTo(10L);

        producer.forgetPipelinesOutside(List.of("b"));

        SumData<LongPointData> onlyB = only(producer.produce(Resource.empty()), RECORDS).getLongSumData();
        assertThat(onlyB.getPoints()).hasSize(2);
        assertThat(onlyB.getPoints()).allSatisfy(point -> assertThat(
                point.getAttributes().get(AttributeKey.stringKey(MetricAttributes.PIPELINE_ID))).isEqualTo("b"));
        assertThat(producer.produce(Resource.empty()))
                .filteredOn(metric -> metric.getName().equals(FactsMetricProducer.STATE_METRIC))
                .singleElement()
                .satisfies(state -> assertThat(state.getLongGaugeData().getPoints()).hasSize(PipelineState.values().length));
    }

    @Test
    void nothingOfferedIsNothingProduced() {
        assertThat(producer.produce(Resource.empty())).isEmpty();
    }
}
