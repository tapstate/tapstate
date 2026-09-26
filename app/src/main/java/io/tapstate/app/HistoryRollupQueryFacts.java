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

/** Process counters for returned history reads that used raw in place of a rollup level. */
final class HistoryRollupQueryFacts {

    private HistoryRollupQueryFacts() { }

    static List<MetricFact> snapshot(Map<Resolution, HistoryRollupQueryHealth.Level> health,
            Instant startedAt, Instant observedAt) {
        Objects.requireNonNull(health, "health");
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(observedAt, "observedAt");
        List<MetricFact> facts = new ArrayList<>();
        counter(facts, "tapstate.process.rollup.query.raw_fallback", "{query}", health,
                HistoryRollupQueryHealth.Level::queries, startedAt, observedAt);
        counter(facts, "tapstate.process.rollup.query.full_raw_fallback", "{query}", health,
                HistoryRollupQueryHealth.Level::fullRawQueries, startedAt, observedAt);
        counter(facts, "tapstate.process.rollup.query.bucket.down_drilled", "{bucket}", health,
                HistoryRollupQueryHealth.Level::buckets, startedAt, observedAt);
        return List.copyOf(facts);
    }

    private static void counter(List<MetricFact> facts, String name, String unit,
            Map<Resolution, HistoryRollupQueryHealth.Level> health,
            ToLongFunction<HistoryRollupQueryHealth.Level> value, Instant startedAt, Instant at) {
        List<MetricPoint> points = new ArrayList<>();
        for (Resolution resolution : Resolution.values()) {
            HistoryRollupQueryHealth.Level level = health.get(resolution);
            if (level != null && value.applyAsLong(level) > 0) {
                points.add(MetricPoint.accumulated(attributes(resolution), startedAt, at,
                        value.applyAsLong(level)));
            }
        }
        if (!points.isEmpty()) {
            facts.add(new MetricFact(name, MetricType.COUNTER, unit, points));
        }
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
}
