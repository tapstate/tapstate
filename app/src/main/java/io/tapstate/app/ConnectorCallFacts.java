package io.tapstate.app;

import io.tapstate.adapters.pdk.PdkExternalCallStats;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Fixed process facts for completed PDK calls observed on this member. */
final class ConnectorCallFacts {

    private ConnectorCallFacts() {
    }

    static List<MetricFact> snapshot(PdkExternalCallStats stats, Instant startedAt, Instant observedAt) {
        Map<PdkExternalCallStats.Call, Map<PdkExternalCallStats.Outcome, PdkExternalCallStats.Reading>> readings =
                stats.snapshot();
        if (readings.isEmpty()) {
            return List.of();
        }
        List<MetricPoint> counts = new ArrayList<>();
        List<MetricPoint> durations = new ArrayList<>();
        for (PdkExternalCallStats.Call call : PdkExternalCallStats.Call.values()) {
            Map<PdkExternalCallStats.Outcome, PdkExternalCallStats.Reading> byOutcome =
                    readings.getOrDefault(call, Map.of());
            for (PdkExternalCallStats.Outcome outcome : PdkExternalCallStats.Outcome.values()) {
                Map<String, String> attributes = Map.of(
                        MetricAttributes.CONNECTOR_CALL, call.label(),
                        MetricAttributes.CONNECTOR_OUTCOME, outcome.label());
                PdkExternalCallStats.Reading reading = byOutcome.get(outcome);
                if (reading == null || reading.count() == 0) {
                    continue;
                }
                counts.add(MetricPoint.accumulated(attributes, startedAt, observedAt, reading.count()));
                durations.add(MetricPoint.distribution(attributes, startedAt, observedAt, reading.duration()));
            }
        }
        if (counts.isEmpty()) {
            return List.of();
        }
        return List.of(
                new MetricFact("tapstate.process.connector.external.call.count", MetricType.COUNTER,
                        "{call}", counts),
                new MetricFact("tapstate.process.connector.external.call.duration", MetricType.HISTOGRAM,
                        "s", durations));
    }
}
