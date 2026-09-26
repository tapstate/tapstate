package io.tapstate.app;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Fixed process facts for lifecycle admission and actual actuator work, independent of pipeline identity. */
final class LifecycleProcessFacts {

    private LifecycleProcessFacts() {
    }

    static List<MetricFact> snapshot(LifecycleWorkDispatcher.Health health, int refusedPending,
            Instant startedAt, Instant observedAt) {
        if (!health.observed()) {
            return List.of();
        }
        List<MetricFact> facts = new ArrayList<>();
        facts.add(gauge("tapstate.process.lifecycle.slots.active", "{slot}", health.activeSlots(), observedAt));
        facts.add(gauge("tapstate.process.lifecycle.pipelines.pending", "{pipeline}",
                health.pendingPipelines() + refusedPending, observedAt));
        facts.add(gauge("tapstate.process.lifecycle.queue.depth", "{pipeline}", health.queueDepth(), observedAt));
        facts.add(gauge("tapstate.process.lifecycle.queue.high_water", "{pipeline}",
                health.queueHighWater(), observedAt));
        facts.add(counter("tapstate.process.lifecycle.intent.coalesced", "{intent}",
                health.coalesced(), startedAt, observedAt));
        facts.add(counter("tapstate.process.lifecycle.intent.cancelled", "{intent}",
                health.cancelled(), startedAt, observedAt));
        facts.add(counter("tapstate.process.lifecycle.capacity.refused", "{offer}",
                health.capacityRefusals(), startedAt, observedAt));
        health.capacityWait().ifPresent(duration -> facts.add(MetricFact.single(
                "tapstate.process.lifecycle.capacity.wait.duration", MetricType.HISTOGRAM, "s",
                MetricPoint.distribution(Map.of(), startedAt, observedAt, duration))));
        List<MetricPoint> work = new ArrayList<>();
        for (LifecycleWorkDispatcher.Verb verb : LifecycleWorkDispatcher.Verb.values()) {
            var duration = health.workDurations().get(verb);
            if (duration != null) {
                work.add(MetricPoint.distribution(Map.of(MetricAttributes.LIFECYCLE_VERB,
                        verb.name().toLowerCase(Locale.ROOT)), startedAt, observedAt, duration));
            }
        }
        if (!work.isEmpty()) {
            facts.add(new MetricFact("tapstate.process.lifecycle.work.duration", MetricType.HISTOGRAM,
                    "s", work));
        }
        return List.copyOf(facts);
    }

    private static MetricFact gauge(String name, String unit, long value, Instant observedAt) {
        return MetricFact.single(name, MetricType.GAUGE, unit,
                MetricPoint.reading(Map.of(), observedAt, value));
    }

    private static MetricFact counter(String name, String unit, long value, Instant startedAt, Instant observedAt) {
        return MetricFact.single(name, MetricType.COUNTER, unit,
                MetricPoint.accumulated(Map.of(), startedAt, observedAt, value));
    }
}
