package io.tapstate.app;

import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.runtime.engine.StoredCountSampler;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Process-local cost of completed namespace counts; no namespace or pipeline becomes an attribute. */
final class StoredCountFacts {

    private StoredCountFacts() {
    }

    static List<MetricFact> snapshot(StoredCountSampler.Health health, Instant startedAt, Instant at) {
        if (health.completed() == 0 && health.failed() == 0 && health.rejected() == 0
                && health.queued() == 0 && health.active() == 0) {
            return List.of();
        }
        return List.of(
                counter("tapstate.process.nest.stored_count.completed", "{query}",
                        health.completed(), startedAt, at),
                counter("tapstate.process.nest.stored_count.failed", "{query}",
                        health.failed(), startedAt, at),
                counter("tapstate.process.nest.stored_count.rejected", "{query}",
                        health.rejected(), startedAt, at),
                counter("tapstate.process.nest.stored_count.duration.sum", "ns",
                        health.totalDurationNanos(), startedAt, at),
                gauge("tapstate.process.nest.stored_count.queued", "{query}", health.queued(), at),
                gauge("tapstate.process.nest.stored_count.active", "{query}", health.active(), at));
    }

    private static MetricFact counter(String name, String unit, long value, Instant startedAt, Instant at) {
        return MetricFact.single(name, MetricType.COUNTER, unit,
                MetricPoint.accumulated(Map.of(), startedAt, at, value));
    }

    private static MetricFact gauge(String name, String unit, long value, Instant at) {
        return MetricFact.single(name, MetricType.GAUGE, unit, MetricPoint.reading(Map.of(), at, value));
    }
}
