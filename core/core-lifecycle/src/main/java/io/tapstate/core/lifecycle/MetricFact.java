package io.tapstate.core.lifecycle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * One metric as the runtime measured it: a name, what kind of measurement it is, its unit, and the
 * points it currently holds. This is the single internal fact that every metric consumer projects
 * from — the human-facing observation a read face serves, and any time-series export — so that the
 * two never keep their own books. Two sets of books on the same quantity drift, and the drift is
 * invisible from either side: each one is internally consistent.
 *
 * <p>The unit rides here as metadata and is never a suffix on the name. Burned into the name it
 * cannot be converted, cannot be compared against the same quantity measured elsewhere, and turns a
 * change of unit into a rename that breaks every consumer.
 *
 * <p>A projection may carry less than this holds — the flat numeric view a command line shows drops
 * buckets, for instance. What it must not do is carry less because the fact never had it: a detail
 * dropped at the projection can be recovered by asking for another projection, and a detail that was
 * never measured is gone. That is the reason this type exists rather than each consumer assembling
 * numbers for itself.
 *
 * <p><strong>Three things a dimensioned or bucketed fact is held to here, beyond its shape.</strong> A
 * distribution carries the bucket bounds registered for its instrument ({@link HistogramBounds}) and no
 * other; a value of a closed attribute — direction, operation, stage — is one of that set's members
 * ({@link MetricAttributes}); and an instrument that carries any attribute at all has a cardinality budget
 * on record ({@link CardinalityBudget}). Each of these is a decision somebody has to make once, and each is
 * silent when skipped: the wrong buckets read as a quantity that never varies, an open value set grows a
 * series per symbol the data invents, and an unbudgeted instrument is capped by whatever exports it.
 *
 * <p><strong>What this deliberately does not check yet.</strong> Attribute <em>keys</em> have a naming
 * discipline of their own — lower-case, dotted, namespaced — and it is not enforced here, because several
 * metric families still carry their one dimension inside the name, appended to a fixed prefix, and the
 * rule lands with their migration rather than ahead of it.
 */
public record MetricFact(String name, MetricType type, String unit, List<MetricPoint> points) {

    public MetricFact {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(unit, "unit");
        Objects.requireNonNull(points, "points");
        if (name.isBlank()) {
            throw new IllegalArgumentException("a metric name must not be blank");
        }
        if (unit.isBlank()) {
            throw new IllegalArgumentException(
                    "a metric carries its unit as metadata; '" + name + "' has none. A unit-less "
                            + "quantity is one whose unit is about to be read off its name.");
        }
        points = List.copyOf(points);
        Set<Map<String, String>> seen = new HashSet<>();
        for (MetricPoint point : points) {
            if (!seen.add(point.attributes())) {
                throw new IllegalArgumentException(
                        "'" + name + "' has two points for the same attributes " + point.attributes()
                                + ": one series cannot hold two values at once, and a consumer "
                                + "picking either one picks arbitrarily");
            }
            if (type == MetricType.HISTOGRAM) {
                if (point.histogram() == null) {
                    throw new IllegalArgumentException(
                            "'" + name + "' is a histogram, so its points carry a distribution, not a "
                                    + "single value");
                }
            } else if (point.value() == null) {
                throw new IllegalArgumentException(
                        "'" + name + "' is a " + type + ", so its points carry a single value, not a "
                                + "distribution");
            }
            if (type == MetricType.COUNTER && point.startTime() == null) {
                throw new IllegalArgumentException(
                        "'" + name + "' accumulates, so every point says what it accumulates from. "
                                + "Without that start a consumer cannot tell an accumulation that "
                                + "began again from one that went backwards.");
            }
        }
        // After the points' own shape, so that a point of the wrong kind is reported as that and never as
        // one of these three.
        if (type == MetricType.HISTOGRAM) {
            HistogramBounds registered = HistogramBounds.forInstrument(name).orElseThrow(() ->
                    new IllegalArgumentException(
                            "'" + name + "' publishes a distribution, and no bucket bounds are registered"
                                    + " for it. Bounds taken from whatever library is in the path measure"
                                    + " that library's idea of the range, not the quantity's."));
            if (!HistogramBounds.UNIT.equals(unit)) {
                throw new IllegalArgumentException(
                        "'" + name + "' is measured in '" + unit + "' over bounds that are given in '"
                                + HistogramBounds.UNIT + "'; a bucket layout is only meaningful in its"
                                + " own unit");
            }
            for (MetricPoint point : points) {
                if (!point.histogram().bounds().equals(registered.bounds())) {
                    throw new IllegalArgumentException(
                            "'" + name + "' carries a distribution over bounds " + point.histogram().bounds()
                                    + " where its registered bounds are " + registered.bounds()
                                    + "; a percentile read across two bucket layouts is a number from"
                                    + " neither");
                }
            }
        }
        boolean dimensioned = false;
        for (MetricPoint point : points) {
            for (Map.Entry<String, String> attribute : point.attributes().entrySet()) {
                dimensioned = true;
                Set<String> domain = MetricAttributes.closedDomain(attribute.getKey()).orElse(null);
                if (domain != null && !domain.contains(attribute.getValue())) {
                    throw new IllegalArgumentException(
                            "'" + name + "' carries " + attribute.getKey() + "=" + attribute.getValue()
                                    + ", which is outside that attribute's closed set "
                                    + new TreeSet<>(domain) + ". A value the data can add is a series"
                                    + " the data can add.");
                }
            }
        }
        if (dimensioned && CardinalityBudget.forInstrument(name).isEmpty()) {
            throw new IllegalArgumentException(
                    "'" + name + "' carries attributes and declares no cardinality budget: how many series"
                            + " it may hold, and on which dimension, is decided by the data until somebody"
                            + " writes the number down.");
        }
    }

    /** A metric holding exactly one point, the shape of a quantity with no dimensions to break out. */
    public static MetricFact single(String name, MetricType type, String unit, MetricPoint point) {
        return new MetricFact(name, type, unit, List.of(point));
    }

    /** This metric with {@code more} appended, for a family assembled one series at a time. */
    public MetricFact with(Collection<MetricPoint> more) {
        List<MetricPoint> combined = new ArrayList<>(points);
        combined.addAll(more);
        return new MetricFact(name, type, unit, combined);
    }
}
