package io.tapstate.app;

import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A store-independent, label-free process projection of cold observation cleanup. */
final class ObservationJanitorFacts {

    private ObservationJanitorFacts() {
    }

    static List<MetricFact> snapshot(ObservationJanitor.Health health, Instant startedAt, Instant observedAt) {
        Objects.requireNonNull(health, "health");
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(observedAt, "observedAt");
        List<MetricFact> facts = new ArrayList<>();
        facts.add(counter("tapstate.process.observation_janitor.scanned", "{document}",
                health.scanned(), startedAt, observedAt));
        facts.add(counter("tapstate.process.observation_janitor.deleted", "{document}",
                health.deleted(), startedAt, observedAt));
        facts.add(counter("tapstate.process.observation_janitor.failure", "{batch}",
                health.failures(), startedAt, observedAt));
        facts.add(gauge("tapstate.process.observation_janitor.batch.duration.max", "ms",
                health.maxDurationMillis(), observedAt));
        if (health.lastSuccessAgeMillis().isPresent()) {
            facts.add(gauge("tapstate.process.observation_janitor.last_success.age", "ms",
                    health.lastSuccessAgeMillis().orElseThrow(), observedAt));
        }
        facts.add(gauge("tapstate.process.observation_janitor.degraded", "1",
                health.degraded() ? 1 : 0, observedAt));
        return List.copyOf(facts);
    }

    private static MetricFact counter(String name, String unit, long value, Instant startedAt, Instant at) {
        return MetricFact.single(name, MetricType.COUNTER, unit,
                MetricPoint.accumulated(Map.of(), startedAt, at, value));
    }

    private static MetricFact gauge(String name, String unit, long value, Instant at) {
        return MetricFact.single(name, MetricType.GAUGE, unit, MetricPoint.reading(Map.of(), at, value));
    }
}
