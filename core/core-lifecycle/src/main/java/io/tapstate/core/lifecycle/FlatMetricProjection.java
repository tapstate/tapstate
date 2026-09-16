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
 * neither survives the trip. That is a limit of this view, not of what was measured: the same facts
 * projected another way keep both.
 *
 * <p>What the loss must never be is quiet. {@link #dropped()} names every metric this view could not
 * represent, so a caller can assert that nothing is being lost today and find out on the day that
 * stops being true, instead of a metric being added and simply not appearing for anyone reading this
 * face. A metric that is absent because nobody wired it and a metric that is absent because the view
 * could not hold it look identical from here, and they need opposite reactions.
 */
public record FlatMetricProjection(Map<String, Long> metrics, List<String> dropped) {

    public FlatMetricProjection {
        metrics = metrics == null ? Map.of() : Map.copyOf(metrics);
        dropped = dropped == null ? List.of() : List.copyOf(dropped);
    }

    /**
     * Projects {@code facts} down to {@code name -> value}, keeping every metric that holds exactly one
     * point with no attributes and reporting the rest as dropped.
     */
    public static FlatMetricProjection of(List<MetricFact> facts) {
        Objects.requireNonNull(facts, "facts");
        Map<String, Long> flat = new LinkedHashMap<>();
        List<String> dropped = new ArrayList<>();
        for (MetricFact fact : facts) {
            if (fact.type() == MetricType.HISTOGRAM || fact.points().size() != 1
                    || !fact.points().get(0).attributes().isEmpty()) {
                dropped.add(fact.name());
                continue;
            }
            flat.put(fact.name(), fact.points().get(0).value());
        }
        return new FlatMetricProjection(flat, dropped);
    }
}
