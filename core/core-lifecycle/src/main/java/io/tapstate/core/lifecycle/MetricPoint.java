package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * One data point of a {@link MetricFact}: the value a metric had for one combination of attributes,
 * at one moment.
 *
 * <ul>
 *   <li>{@code attributes} — the dimensions this point is broken down by, empty when the metric has
 *       only one series. Dimensions live here and never in the metric's name: a name per combination
 *       multiplies into a family of names that no consumer can group, filter or sum across, and that
 *       has to be extended by hand every time a new value appears.</li>
 *   <li>{@code startTime} — when this point's accumulation began, for an accumulating metric.
 *       {@code null} for a value that is simply read at a moment. {@link MetricFact} requires it on
 *       an accumulating metric and refuses the fact without it, rather than letting the absence
 *       travel to a consumer who cannot tell a restart from a decrease.</li>
 *   <li>{@code observedAt} — when the value was taken. Required: a point with no time is a number
 *       that cannot be placed against any other number, and a consumer that supplies its own clock
 *       instead reports a stale reading as current.</li>
 *   <li>{@code value} — the number, for a counter or a gauge.</li>
 *   <li>{@code histogram} — the distribution, for a histogram.</li>
 * </ul>
 *
 * <p>A point carries exactly one of {@code value} and {@code histogram}. Both would be two answers to
 * the same question with nothing to say which one is meant; neither is a point that measured nothing,
 * which is not the same as a metric having no points and must not be encoded as one.
 */
public record MetricPoint(Map<String, String> attributes, Instant startTime, Instant observedAt,
        Long value, HistogramValue histogram) {

    public MetricPoint {
        Objects.requireNonNull(observedAt, "observedAt");
        if ((value == null) == (histogram == null)) {
            throw new IllegalArgumentException(
                    "a point carries exactly one of a value and a histogram, never both and never "
                            + "neither");
        }
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        for (Map.Entry<String, String> attribute : attributes.entrySet()) {
            if (attribute.getKey().isBlank()) {
                throw new IllegalArgumentException("an attribute key must not be blank");
            }
            Objects.requireNonNull(attribute.getValue(), "attribute value for " + attribute.getKey());
        }
    }

    /** A point read at a moment, with no accumulation behind it. */
    public static MetricPoint reading(Map<String, String> attributes, Instant observedAt, long value) {
        return new MetricPoint(attributes, null, observedAt, value, null);
    }

    /** A point of an accumulating stream, carrying the start its accumulation is counted from. */
    public static MetricPoint accumulated(Map<String, String> attributes, Instant startTime,
            Instant observedAt, long value) {
        return new MetricPoint(attributes, Objects.requireNonNull(startTime, "startTime"), observedAt,
                value, null);
    }

    /** A point carrying a distribution rather than a single number. */
    public static MetricPoint distribution(Map<String, String> attributes, Instant startTime,
            Instant observedAt, HistogramValue histogram) {
        return new MetricPoint(attributes, startTime, observedAt, null,
                Objects.requireNonNull(histogram, "histogram"));
    }
}
