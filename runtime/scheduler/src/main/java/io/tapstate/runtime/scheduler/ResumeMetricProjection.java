package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.ResumeMetrics;
import io.tapstate.spi.store.ResumeMetrics.Producer;
import io.tapstate.spi.store.ResumeMetrics.Slot;
import io.tapstate.spi.store.ResumeMetrics.Window;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class ResumeMetricProjection {
    private ResumeMetricProjection() {}

    record Result(List<MetricFact> facts, ResumeMetrics.Publication publication) {}

    static Result project(PipelineIntentScope scope, ResumeMetrics.Checkpoint checkpoint, ObservationPublisher.NativeSample sample) {
        var prior = checkpoint.prior();
        if (prior == null) return null;
        var binding = checkpoint.applied();
        var key = new ResumeMetrics.ObservationKey(scope, checkpoint.state().epoch(), binding.fence());
        if (sample == null) {
            // While submission is pending the new key may publish lifecycle state without movement.
            return prior.exists() ? null : new Result(List.of(), new ResumeMetrics.Publication(key, prior, null));
        }
        var window = window(binding.revision(), sample.jobId(), prior, sample.starts());
        // A legacy result without native evidence must survive until its producer can be measured.
        if (!window.slots().keySet().containsAll(prior.cumulative())) return null;
        return new Result(apply(binding.baseline(), sample.facts(), window), new ResumeMetrics.Publication(key, prior, window));
    }

    static Window window(long revision, long jobId, ResumeMetrics.Prior prior, Map<Producer, Long> starts) {
        var slots = new EnumMap<Producer, Slot>(Producer.class);
        var old = prior.window();
        boolean sameRun = old != null && old.jobId() == jobId && old.intentRevision() == revision;
        if (sameRun) slots.putAll(old.slots());
        starts.forEach((producer, start) -> {
            var previous = slots.get(producer);
            boolean carry = previous == null
                    ? (old == null || sameRun) && !prior.cumulative().contains(producer)
                    : previous.carry() && previous.startMillis() == start;
            slots.put(producer, new Slot(start, carry));
        });
        return new Window(revision, jobId, slots);
    }

    static List<MetricFact> apply(ResumeMetrics.Baseline baseline, List<MetricFact> raw, Window window) {
        var previous = ResumeMetrics.index(baseline.facts());
        var instruments = new LinkedHashMap<String, MetricFact>();
        var pointsByName = new LinkedHashMap<String, List<MetricPoint>>();
        var windows = new EnumMap<Producer, MetricPoint>(Producer.class);
        for (var fact : raw) {
            instruments.put(fact.name(), fact);
            var points = pointsByName.computeIfAbsent(fact.name(), ignored -> new ArrayList<>());
            for (var point : fact.points()) {
                if (point.startTime() == null) {
                    points.add(point);
                    continue;
                }
                if (baseline.unavailable()) continue;
                var producer = producer(fact, point);
                var slot = window.slots().get(producer);
                if (slot == null || !slot.carry() || slot.startMillis() != point.startTime().toEpochMilli()) {
                    points.add(point);
                    continue;
                }
                windows.put(producer, point);
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
                var latest = windows.get(producer(fact, old));
                if (latest == null || !previous.containsKey(new ResumeMetrics.Series(fact.name(), old.attributes()))) continue;
                instruments.putIfAbsent(fact.name(), fact);
                pointsByName.computeIfAbsent(fact.name(), ignored -> new ArrayList<>()).add(new MetricPoint(
                        old.attributes(), old.startTime(), latest.observedAt(), old.value(), old.histogram()));
            }
        }
        var result = new ArrayList<MetricFact>();
        instruments.forEach((name, fact) -> {
            var points = pointsByName.get(name);
            if (!points.isEmpty()) result.add(new MetricFact(name, fact.type(), fact.unit(), points));
        });
        return List.copyOf(result);
    }

    private static Producer producer(MetricFact fact, MetricPoint point) {
        var direction = point.attributes().get(io.tapstate.core.lifecycle.MetricAttributes.DIRECTION);
        if (direction != null) return "in".equals(direction) ? Producer.IN : Producer.OUT;
        return fact.name().equals("tapstate.pipeline.record.delivery.duration") ? Producer.OUT : Producer.STAGE;
    }

    private static HistogramValue add(HistogramValue left, HistogramValue right) {
        var buckets = new ArrayList<Long>(left.bucketCounts().size());
        for (int i = 0; i < left.bucketCounts().size(); i++)
            buckets.add(Math.addExact(left.bucketCounts().get(i), right.bucketCounts().get(i)));
        return new HistogramValue(Math.addExact(left.count(), right.count()), left.sum() + right.sum(),
                left.bounds(), buckets);
    }
}
