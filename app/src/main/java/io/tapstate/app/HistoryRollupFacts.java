package io.tapstate.app;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.spi.store.HistoryRollupStore.Resolution;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToLongFunction;

/** A bounded process projection of the cache worker, independent of its storage calls. */
final class HistoryRollupFacts {

    private HistoryRollupFacts() { }

    static List<MetricFact> snapshot(HistoryRollupWorker.Health health, Instant startedAt, Instant observedAt) {
        Objects.requireNonNull(health, "health");
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(observedAt, "observedAt");
        if (health.levels().isEmpty() && !health.degraded()) {
            return List.of();
        }
        List<MetricFact> facts = new ArrayList<>();
        if (!health.levels().isEmpty()) {
            facts.add(counter("tapstate.process.rollup.bucket.computed", "{bucket}", health,
                    HistoryRollupWorker.LevelHealth::computed, startedAt, observedAt));
            facts.add(counter("tapstate.process.rollup.bucket.retried", "{bucket}", health,
                    HistoryRollupWorker.LevelHealth::retried, startedAt, observedAt));
            facts.add(counter("tapstate.process.rollup.bucket.failed", "{bucket}", health,
                    HistoryRollupWorker.LevelHealth::failed, startedAt, observedAt));
            facts.add(counter("tapstate.process.rollup.build.raw_fallback", "{bucket}", health,
                    HistoryRollupWorker.LevelHealth::rawFallback, startedAt, observedAt));
            List<MetricPoint> ages = new ArrayList<>();
            for (Resolution resolution : Resolution.values()) {
                HistoryRollupWorker.LevelHealth level = health.levels().get(resolution);
                if (level != null && level.closedThroughAgeMillis().isPresent()) {
                    ages.add(MetricPoint.reading(attributes(resolution), observedAt,
                            level.closedThroughAgeMillis().orElseThrow()));
                }
            }
            if (!ages.isEmpty()) {
                facts.add(new MetricFact("tapstate.process.rollup.closed_through.age", MetricType.GAUGE,
                        "ms", ages));
            }
        }
        if (health.maxBatchDurationMillis().isPresent()) {
            facts.add(gauge("tapstate.process.rollup.batch.duration.max", "ms",
                    health.maxBatchDurationMillis().orElseThrow(), observedAt));
        }
        if (health.inFlightAgeMillis().isPresent()) {
            facts.add(gauge("tapstate.process.rollup.batch.in_flight.age", "ms",
                    health.inFlightAgeMillis().orElseThrow(), observedAt));
        }
        facts.add(gauge("tapstate.process.rollup.degraded", "1",
                health.degraded() ? 1 : 0, observedAt));
        return List.copyOf(facts);
    }

    private static MetricFact counter(String name, String unit, HistoryRollupWorker.Health health,
            ToLongFunction<HistoryRollupWorker.LevelHealth> value, Instant startedAt, Instant at) {
        List<MetricPoint> points = new ArrayList<>();
        for (Resolution resolution : Resolution.values()) {
            HistoryRollupWorker.LevelHealth level = health.levels().get(resolution);
            if (level != null) {
                points.add(MetricPoint.accumulated(attributes(resolution), startedAt, at,
                        value.applyAsLong(level)));
            }
        }
        return new MetricFact(name, MetricType.COUNTER, unit, points);
    }

    private static Map<String, String> attributes(Resolution resolution) {
        String label = switch (resolution) {
            case PT5M -> "5m";
            case PT30M -> "30m";
            case PT1H -> "1h";
            case PT3H -> "3h";
            case PT6H -> "6h";
        };
        return Map.of(MetricAttributes.ROLLUP_RESOLUTION, label);
    }

    private static MetricFact gauge(String name, String unit, long value, Instant at) {
        return MetricFact.single(name, MetricType.GAUGE, unit, MetricPoint.reading(Map.of(), at, value));
    }
}
