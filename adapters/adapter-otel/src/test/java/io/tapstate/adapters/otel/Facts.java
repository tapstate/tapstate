package io.tapstate.adapters.otel;

import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** The facts the cases here offer: a records counter over two directions, a lag gauge, a delivery histogram. */
final class Facts {

    static final String RECORDS = "tapstate.pipeline.records";
    static final String LAG = "tapstate.pipeline.lag";
    static final String DELIVERY = "tapstate.pipeline.record.delivery.duration";
    static final Instant START = Instant.parse("2026-09-18T08:00:00Z");
    static final Instant AT = START.plusSeconds(60);

    private Facts() {
    }

    static Map<String, String> table(String pipelineId, String table) {
        return Map.of(MetricAttributes.PIPELINE_ID, pipelineId, MetricAttributes.TABLE_ID, table);
    }

    static Map<String, String> tableAndDirection(String pipelineId, String table, String direction) {
        return Map.of(MetricAttributes.PIPELINE_ID, pipelineId, MetricAttributes.TABLE_ID, table,
                MetricAttributes.DIRECTION, direction);
    }

    static MetricFact records(String pipelineId, long out, long in) {
        return new MetricFact(RECORDS, MetricType.COUNTER, "{record}", List.of(
                MetricPoint.accumulated(tableAndDirection(pipelineId, "orders", "out"), START, AT, out),
                MetricPoint.accumulated(tableAndDirection(pipelineId, "orders", "in"), START, AT, in)));
    }

    static MetricFact lag(String pipelineId, long seconds) {
        return new MetricFact(LAG, MetricType.GAUGE, "s", List.of(
                MetricPoint.reading(table(pipelineId, "orders"), AT, seconds)));
    }

    /** The bounds the delivery histogram is registered with; a fact over any others is refused where it is built. */
    static final List<Double> DELIVERY_BOUNDS = HistogramBounds.forInstrument(DELIVERY).orElseThrow().bounds();

    /** Three deliveries: one in the first bucket, two in the bucket bounded by one second, none elsewhere. */
    static List<Long> deliveryCounts() {
        Long[] counts = new Long[DELIVERY_BOUNDS.size() + 1];
        java.util.Arrays.fill(counts, 0L);
        counts[0] = 1L;
        counts[DELIVERY_BOUNDS.indexOf(1.0)] = 2L;
        return List.of(counts);
    }

    static MetricFact delivery(String pipelineId) {
        return new MetricFact(DELIVERY, MetricType.HISTOGRAM, "s", List.of(
                MetricPoint.distribution(table(pipelineId, "orders"), START, AT,
                        new HistogramValue(3, 1.5, DELIVERY_BOUNDS, deliveryCounts()))));
    }

    static long nanos(Instant instant) {
        return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
    }
}
