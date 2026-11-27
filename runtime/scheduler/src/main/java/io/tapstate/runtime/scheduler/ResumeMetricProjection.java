package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.spi.store.ResumeMetrics;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.LinkedHashMap;

final class ResumeMetricProjection {
    private ResumeMetricProjection() {}

    static List<MetricFact> apply(ResumeMetrics.Baseline baseline, List<MetricFact> raw) {
        var previous = ResumeMetrics.index(baseline.facts());
        var instruments = new LinkedHashMap<String, MetricFact>();
        var pointsByName = new LinkedHashMap<String, List<MetricPoint>>();
        var windows = new HashMap<String, MetricPoint>();
        for (var fact : raw) {
            instruments.put(fact.name(), fact);
            var points = pointsByName.computeIfAbsent(fact.name(), ignored -> new ArrayList<>());
            for (var point : fact.points()) {
                if (point.startTime() == null) {
                    points.add(point);
                    continue;
                }
                if (baseline.unavailable()) continue;
                windows.put(producer(fact, point), point);
                var old = previous.remove(new ResumeMetrics.Series(fact.name(), point.attributes()));
                if (old == null) points.add(point);
                else if (point.histogram() == null)
                    points.add(MetricPoint.accumulated(point.attributes(), old.startTime(), point.observedAt(),
                            Math.addExact(old.value(), point.value())));
                else if (old.histogram().bounds().equals(point.histogram().bounds()))
                    points.add(MetricPoint.distribution(point.attributes(), old.startTime(), point.observedAt(),
                            add(old.histogram(), point.histogram())));
            }
        }
        for (var fact : baseline.facts()) {
            for (var old : fact.points()) {
                var window = windows.get(producer(fact, old));
                if (window == null || !previous.containsKey(new ResumeMetrics.Series(fact.name(), old.attributes()))) continue;
                instruments.putIfAbsent(fact.name(), fact);
                pointsByName.computeIfAbsent(fact.name(), ignored -> new ArrayList<>()).add(new MetricPoint(
                        old.attributes(), old.startTime(), window.observedAt(), old.value(), old.histogram()));
            }
        }
        var result = new ArrayList<MetricFact>();
        instruments.forEach((name, fact) -> {
            var points = pointsByName.get(name);
            if (!points.isEmpty()) result.add(new MetricFact(name, fact.type(), fact.unit(), points));
        });
        return List.copyOf(result);
    }

    private static String producer(MetricFact fact, MetricPoint point) {
        var direction = point.attributes().get(io.tapstate.core.lifecycle.MetricAttributes.DIRECTION);
        if (direction != null) return direction;
        return fact.name().equals("tapstate.pipeline.record.delivery.duration") ? "out" : fact.name();
    }

    private static HistogramValue add(HistogramValue left, HistogramValue right) {
        var buckets = new ArrayList<Long>(left.bucketCounts().size());
        for (int i = 0; i < left.bucketCounts().size(); i++)
            buckets.add(Math.addExact(left.bucketCounts().get(i), right.bucketCounts().get(i)));
        return new HistogramValue(Math.addExact(left.count(), right.count()), left.sum() + right.sum(),
                left.bounds(), buckets);
    }
}
