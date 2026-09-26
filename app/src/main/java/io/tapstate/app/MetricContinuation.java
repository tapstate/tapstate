package io.tapstate.app;

import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Known cumulative points captured before a paused job is rebuilt. */
final class MetricContinuation {

    private final Map<String, MetricFact> baseline;

    private MetricContinuation(Map<String, MetricFact> baseline) {
        this.baseline = Map.copyOf(baseline);
    }

    static MetricContinuation capture(Observation observation) {
        Map<String, MetricFact> known = new LinkedHashMap<>();
        if (observation != null) {
            for (MetricFact fact : observation.facts()) {
                if (fact.type() != MetricType.GAUGE && !fact.points().isEmpty()) {
                    known.put(fact.name(), fact);
                }
            }
        }
        return new MetricContinuation(known);
    }

    boolean isEmpty() {
        return baseline.isEmpty();
    }

    MetricContinuation boundedBy(CardinalityBudget.Folder folder) {
        Map<String, MetricFact> bounded = new LinkedHashMap<>();
        baseline.values().forEach(fact -> bounded.put(fact.name(), folder.fold(fact)));
        return new MetricContinuation(bounded);
    }

    List<MetricFact> apply(List<MetricFact> measured, Instant at) {
        Objects.requireNonNull(measured, "measured");
        Objects.requireNonNull(at, "at");
        Map<String, MetricFact> remaining = new LinkedHashMap<>(baseline);
        List<MetricFact> result = new ArrayList<>(measured.size() + baseline.size());
        for (MetricFact current : measured) {
            MetricFact previous = remaining.remove(current.name());
            if (current.type() == MetricType.GAUGE || previous == null
                    || previous.type() != current.type() || !previous.unit().equals(current.unit())) {
                result.add(current);
                continue;
            }
            Map<Map<String, String>, MetricPoint> oldPoints = new LinkedHashMap<>();
            for (MetricPoint old : previous.points()) {
                oldPoints.put(old.attributes(), old);
            }
            List<MetricPoint> combined = new ArrayList<>(current.points().size() + oldPoints.size());
            for (MetricPoint fresh : current.points()) {
                MetricPoint old = oldPoints.remove(fresh.attributes());
                combined.add(old == null ? fresh : add(current.type(), old, fresh, at));
            }
            oldPoints.values().forEach(old -> combined.add(at(old, current.type(), at)));
            result.add(new MetricFact(current.name(), current.type(), current.unit(), combined));
        }
        remaining.values().forEach(old -> result.add(new MetricFact(old.name(), old.type(), old.unit(),
                old.points().stream().map(point -> at(point, old.type(), at)).toList())));
        return List.copyOf(result);
    }

    /** Holds the last known total when a later producer reading temporarily omits or lags a point. */
    List<MetricFact> atLeast(List<MetricFact> measured, Instant at) {
        Map<String, MetricFact> remaining = new LinkedHashMap<>(baseline);
        List<MetricFact> result = new ArrayList<>(measured.size() + baseline.size());
        for (MetricFact current : measured) {
            MetricFact previous = remaining.remove(current.name());
            if (current.type() == MetricType.GAUGE || previous == null
                    || previous.type() != current.type() || !previous.unit().equals(current.unit())) {
                result.add(current);
                continue;
            }
            Map<Map<String, String>, MetricPoint> oldPoints = new LinkedHashMap<>();
            previous.points().forEach(point -> oldPoints.put(point.attributes(), point));
            List<MetricPoint> kept = new ArrayList<>(current.points().size() + oldPoints.size());
            for (MetricPoint fresh : current.points()) {
                MetricPoint old = oldPoints.remove(fresh.attributes());
                if (old == null || !behind(current.type(), fresh, old)) {
                    kept.add(fresh);
                } else {
                    kept.add(at(old, current.type(), at));
                }
            }
            oldPoints.values().forEach(old -> kept.add(at(old, current.type(), at)));
            result.add(new MetricFact(current.name(), current.type(), current.unit(), kept));
        }
        remaining.values().forEach(old -> result.add(new MetricFact(old.name(), old.type(), old.unit(),
                old.points().stream().map(point -> at(point, old.type(), at)).toList())));
        return List.copyOf(result);
    }

    private static boolean behind(MetricType type, MetricPoint current, MetricPoint previous) {
        if (type == MetricType.COUNTER) {
            return current.value() < previous.value();
        }
        HistogramValue now = current.histogram();
        HistogramValue before = previous.histogram();
        if (now.count() < before.count()) {
            return true;
        }
        for (int index = 0; index < now.bucketCounts().size(); index++) {
            if (now.bucketCounts().get(index) < before.bucketCounts().get(index)) {
                return true;
            }
        }
        return false;
    }

    private static MetricPoint add(MetricType type, MetricPoint old, MetricPoint fresh, Instant at) {
        Instant start = old.startTime() == null ? fresh.startTime() : old.startTime();
        if (type == MetricType.COUNTER) {
            return MetricPoint.accumulated(fresh.attributes(), start, at,
                    Math.addExact(old.value(), fresh.value()));
        }
        HistogramValue before = old.histogram();
        HistogramValue after = fresh.histogram();
        if (!before.bounds().equals(after.bounds())) {
            throw new IllegalArgumentException("a histogram cannot continue across different bucket bounds");
        }
        List<Long> buckets = new ArrayList<>(before.bucketCounts().size());
        for (int index = 0; index < before.bucketCounts().size(); index++) {
            buckets.add(Math.addExact(before.bucketCounts().get(index), after.bucketCounts().get(index)));
        }
        return MetricPoint.distribution(fresh.attributes(), start, at,
                new HistogramValue(Math.addExact(before.count(), after.count()), before.sum() + after.sum(),
                        before.bounds(), buckets));
    }

    private static MetricPoint at(MetricPoint point, MetricType type, Instant at) {
        return type == MetricType.COUNTER
                ? MetricPoint.accumulated(point.attributes(), point.startTime(), at, point.value())
                : MetricPoint.distribution(point.attributes(), point.startTime(), at, point.histogram());
    }
}
