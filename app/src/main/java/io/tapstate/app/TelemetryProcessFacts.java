package io.tapstate.app;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.ToLongFunction;

/** A fixed, store-independent process projection of the bounded telemetry workers' health. */
final class TelemetryProcessFacts {

    private record Reading(TelemetryDispatcher.Sink sink, TelemetryDispatcher.Health health) {
        private Map<String, String> attributes() {
            return Map.of(MetricAttributes.TELEMETRY_SINK, sink.name().toLowerCase(Locale.ROOT));
        }
    }

    private TelemetryProcessFacts() {
    }

    static List<MetricFact> snapshot(Map<TelemetryDispatcher.Sink, TelemetryDispatcher.Health> health,
            Set<TelemetryDispatcher.Sink> wired, Instant startedAt, Instant observedAt) {
        Objects.requireNonNull(health, "health");
        Objects.requireNonNull(wired, "wired");
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(observedAt, "observedAt");
        List<Reading> active = new ArrayList<>();
        for (TelemetryDispatcher.Sink sink : EnumSet.allOf(TelemetryDispatcher.Sink.class)) {
            TelemetryDispatcher.Health reading = health.get(sink);
            if (wired.contains(sink) && reading != null) {
                active.add(new Reading(sink, reading));
            }
        }
        if (active.isEmpty()) {
            return List.of();
        }
        List<MetricFact> facts = new ArrayList<>();
        facts.add(gauge("tapstate.process.telemetry.queue.depth", "{task}", active,
                reading -> reading.queueDepth(), observedAt));
        facts.add(gauge("tapstate.process.telemetry.queue.high_water", "{task}", active,
                TelemetryDispatcher.Health::highWater, observedAt));
        facts.add(gauge("tapstate.process.telemetry.in_flight", "{task}", active,
                reading -> reading.inFlight(), observedAt));
        facts.add(counter("tapstate.process.telemetry.coalesced", "{frame}", active,
                TelemetryDispatcher.Health::coalesced, startedAt, observedAt));
        facts.add(counter("tapstate.process.telemetry.dropped", "{frame}", active,
                TelemetryDispatcher.Health::dropped, startedAt, observedAt));
        facts.add(counter("tapstate.process.telemetry.write.success", "{write}", active,
                TelemetryDispatcher.Health::successes, startedAt, observedAt));
        facts.add(counter("tapstate.process.telemetry.write.failure", "{write}", active,
                TelemetryDispatcher.Health::failures, startedAt, observedAt));
        facts.add(counter("tapstate.process.telemetry.write.timeout", "{write}", active,
                TelemetryDispatcher.Health::timeouts, startedAt, observedAt));
        facts.add(gauge("tapstate.process.telemetry.write.duration.max", "ms", active,
                TelemetryDispatcher.Health::maxDurationMillis, observedAt));
        List<MetricPoint> ages = active.stream().filter(reading ->
                reading.health().lastSuccessAgeMillis().isPresent()).map(reading -> MetricPoint.reading(
                reading.attributes(), observedAt, reading.health().lastSuccessAgeMillis().orElseThrow())).toList();
        if (!ages.isEmpty()) {
            facts.add(new MetricFact("tapstate.process.telemetry.last_success.age",
                    MetricType.GAUGE, "ms", ages));
        }
        facts.add(gauge("tapstate.process.telemetry.degraded", "1", active,
                reading -> reading.degraded() ? 1L : 0L, observedAt));
        return List.copyOf(facts);
    }

    private static MetricFact gauge(String name, String unit, List<Reading> active,
            ToLongFunction<TelemetryDispatcher.Health> value, Instant at) {
        return new MetricFact(name, MetricType.GAUGE, unit, active.stream().map(reading ->
                MetricPoint.reading(reading.attributes(), at, value.applyAsLong(reading.health()))).toList());
    }

    private static MetricFact counter(String name, String unit, List<Reading> active,
            ToLongFunction<TelemetryDispatcher.Health> value, Instant startedAt, Instant at) {
        return new MetricFact(name, MetricType.COUNTER, unit, active.stream().map(reading ->
                MetricPoint.accumulated(reading.attributes(), startedAt, at,
                        value.applyAsLong(reading.health()))).toList());
    }
}
