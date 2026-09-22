package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.control.core.PipelineMetrics;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * The metrics read face on the wire: the pipeline id, the open map of numeric run statistics, the per-table
 * target-acked source position ({@code table -> opaque srcpos}), the names of the positions this product
 * does not record, and the facts the statistics were measured as.
 *
 * <p>The position rides beside the metrics map rather than inside it. A position is a string and every
 * metrics cell is a number, so nesting them put one string-valued cell in an otherwise numeric map and made
 * every reader type-test a cell before using it — the control read model already keeps the two apart, and
 * this is the wire saying the same thing. The position is absent until one is acked, mirroring the
 * never-faked, empty-is-unavailable rule the numeric metrics follow.
 *
 * <p><strong>The field is named for the position it carries.</strong> {@code targetAckedPosition} is how
 * far the target has confirmed writes; it is not how far the source could be read to and not how far the
 * pipeline has processed, and it must not be read as either. Those two are the ones a reader assumes when
 * a field is called just an offset, and on a run whose target has stopped accepting writes they are the
 * two that keep moving while this one does not.
 *
 * <p><strong>{@code positionsNotCollected} names what is missing instead of leaving it out.</strong> A
 * position that simply does not appear reads the same as a position the product has no concept of, and a
 * caller cannot act on the difference it cannot see. Listing the names makes "we do not measure this" a
 * reading in its own right, and keeps a future wiring of one of them an addition a caller can notice
 * rather than a field that quietly starts appearing.
 *
 * <p><strong>{@code facts} carries the measurements whole, beside the flat map that squeezes them.</strong>
 * One element per metric: its canonical name, what kind of measurement it is ({@code counter},
 * {@code gauge} or {@code histogram}), its unit, and its points — each with the attributes it is broken
 * down by, when it was taken, what an accumulation is counted from, and either a single value or the four
 * parts of a distribution (the count, the sum, the bucket bounds and the count in each bucket, with one
 * more count than bounds for everything above the last one). The flat map stays for every reader that
 * wants one number under one name; a reader that wants to group by table, filter by direction or take a
 * percentile reads here. Empty when the publisher recorded none, never derived from the flat map.
 *
 * <p><strong>This shape is a contract, pinned by a golden, and it is built to be pinnable.</strong> Every
 * value is a string, a number, a list or an object of those: the instants are written here as ISO-8601
 * text rather than left for the JSON codec to format, the kind is the lower-case word rather than an enum
 * for the codec to name, and every map is sorted by key. So the bytes this renders to are a function of the
 * record and not of how the codec happens to be configured — the same on every server and on every run —
 * and a golden written against them stays a golden of the wire rather than of a mapper's defaults.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record PipelineMetricsResponse(String pipelineId, Map<String, Long> metrics,
        Map<String, String> targetAckedPosition, List<String> positionsNotCollected, List<Fact> facts) {

    /** One metric as measured: what it is called, what kind of measurement, in what unit, and its points. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Fact(String name, String type, String unit, List<Point> points) {
    }

    /**
     * One point of a metric. {@code value} is present for a counter or a gauge; {@code count}, {@code sum},
     * {@code bounds} and {@code bucketCounts} together for a histogram; {@code startTime} only for a point
     * that accumulates. Absent fields are absent, never null, so presence alone says which kind this is.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Point(Map<String, String> attributes, String startTime, String observedAt, Long value,
            Long count, Double sum, List<Double> bounds, List<Long> bucketCounts) {
    }

    public static PipelineMetricsResponse of(PipelineMetrics metrics) {
        // Sorted so that two renderings of one observation are one byte sequence: the read model's maps
        // are copies whose iteration order is not a promise, and a wire whose key order changed from one
        // read to the next would be a wire no golden could pin and no diff could read.
        return new PipelineMetricsResponse(metrics.pipelineId(), new TreeMap<>(metrics.metrics()),
                metrics.targetAckedPosition().isEmpty() ? null : new TreeMap<>(metrics.targetAckedPosition()),
                PipelineMetrics.POSITIONS_NOT_COLLECTED,
                metrics.facts().stream()
                        .sorted(Comparator.comparing(MetricFact::name))
                        .map(PipelineMetricsResponse::fact)
                        .toList());
    }

    private static Fact fact(MetricFact fact) {
        return new Fact(fact.name(), fact.type().name().toLowerCase(Locale.ROOT), fact.unit(),
                fact.points().stream()
                        .map(PipelineMetricsResponse::point)
                        // By attributes, which are what make a point one series rather than another;
                        // the facts already refuse two points on the same attributes, so this is a total order.
                        .sorted(Comparator.comparing(point -> point.attributes().toString()))
                        .toList());
    }

    private static Point point(MetricPoint point) {
        HistogramValue histogram = point.histogram();
        return new Point(new TreeMap<>(point.attributes()), text(point.startTime()), text(point.observedAt()),
                point.value(),
                histogram == null ? null : histogram.count(),
                histogram == null ? null : histogram.sum(),
                histogram == null ? null : histogram.bounds(),
                histogram == null ? null : histogram.bucketCounts());
    }

    /** ISO-8601 instant text, which is also what the status face's codec-formatted instant renders as. */
    private static String text(Instant instant) {
        return instant == null ? null : instant.toString();
    }
}
