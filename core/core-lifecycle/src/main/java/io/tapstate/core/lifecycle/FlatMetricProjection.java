package io.tapstate.core.lifecycle;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The flat numeric view of a set of {@link MetricFact}s: {@code name -> value}, which is the shape the
 * stored observation and the read faces on top of it have always carried.
 *
 * <p>It is a projection, and a lossy one. A distribution has no single number to be, and a metric
 * broken out by attributes has one value per combination with only one name to put them under — so
 * neither survives the trip whole. That is a limit of this view, not of what was measured: the same facts
 * projected another way keep both.
 *
 * <p>A caller that would rather have a squeezed number than none may supply a {@link FlatReduction} per
 * metric, naming the flat key each point belongs under. Points that land on one key are then combined the
 * way their metric's type allows: a counter measures work and its points add up, while a gauge is a
 * reading and readings do not — several of them keep the highest, which for every quantity published this
 * way is the one an operator would act on. A distribution is refused a rule outright, because no key turns
 * a shape into a number.
 *
 * <p>What the loss must never be is quiet, in either form. {@link #dropped()} names every metric this view
 * could not represent at all, and {@link #reduced()} names every one it could only represent by collapsing
 * — so a caller can assert that nothing is being lost today and find out on the day that stops being true,
 * instead of a metric being added and simply not appearing, or appearing as a number that is not the one
 * its name suggests. The two lists are kept apart because they call for different answers: a drop wants
 * another face to carry the metric, while a reduction wants whoever reads this one told what they are
 * reading. A metric appears in at most one of them.
 */
public record FlatMetricProjection(Map<String, Long> metrics, List<String> dropped, List<String> reduced) {

    public FlatMetricProjection {
        metrics = metrics == null ? Map.of() : Map.copyOf(metrics);
        dropped = dropped == null ? List.of() : List.copyOf(dropped);
        reduced = reduced == null ? List.of() : List.copyOf(reduced);
    }

    /**
     * Projects {@code facts} down to {@code name -> value}, keeping every metric that holds exactly one
     * point with no attributes and reporting the rest as dropped. Nothing is reduced: with no rule to
     * squeeze a dimensioned metric by, there is nothing this view can do with one but say so.
     */
    public static FlatMetricProjection of(List<MetricFact> facts) {
        return of(facts, Map.of());
    }

    /**
     * Projects {@code facts} down to {@code name -> value}, applying {@code reductions} to the metrics
     * this view could not otherwise carry. A metric that already fits is carried whole and is not reduced,
     * whether or not a rule names it — a rule is a way to carry what would be lost, never a way to rename
     * what arrives.
     *
     * @throws IllegalArgumentException if a rule has no key for a point it is handed
     */
    public static FlatMetricProjection of(List<MetricFact> facts, Map<String, FlatReduction> reductions) {
        Objects.requireNonNull(facts, "facts");
        Objects.requireNonNull(reductions, "reductions");
        Map<String, Long> flat = new LinkedHashMap<>();
        List<String> dropped = new ArrayList<>();
        List<String> reduced = new ArrayList<>();
        for (MetricFact fact : facts) {
            if (carriedWhole(fact)) {
                flat.put(fact.name(), fact.points().get(0).value());
                continue;
            }
            FlatReduction reduction =
                    fact.type() == MetricType.HISTOGRAM ? null : reductions.get(fact.name());
            if (reduction == null) {
                dropped.add(fact.name());
                continue;
            }
            collapse(fact, reduction, flat);
            reduced.add(fact.name());
        }
        return new FlatMetricProjection(flat, dropped, reduced);
    }

    /** Whether this view holds the fact as measured: one number, under its own name, losing nothing. */
    private static boolean carriedWhole(MetricFact fact) {
        return fact.type() != MetricType.HISTOGRAM && fact.points().size() == 1
                && fact.points().get(0).attributes().isEmpty();
    }

    /**
     * Writes {@code fact}'s points into {@code flat} under the keys {@code reduction} gives them, combining
     * the points that share a key by what the metric's type makes of them.
     */
    private static void collapse(MetricFact fact, FlatReduction reduction, Map<String, Long> flat) {
        for (MetricPoint point : fact.points()) {
            String key = reduction.keyFor(point.attributes());
            if (key == null) {
                throw new IllegalArgumentException("no flat key for a point of " + fact.name()
                        + " carrying attributes " + point.attributes()
                        + "; a reduction answers for every point, since a total short by the ones it"
                        + " skipped reads exactly like a correct one");
            }
            flat.merge(key, point.value(),
                    fact.type() == MetricType.COUNTER ? Long::sum : Math::max);
        }
    }
}
