package io.tapstate.app;

import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Native producer epochs stay separate from public starts; only budgeted, folded facts are retained. */
final class MetricProducerEpochs {
    private record Producer(String direction, String stage) {
        private static Producer of(MetricPoint point) {
            return new Producer(point.attributes().getOrDefault(MetricAttributes.DIRECTION, ""),
                    point.attributes().getOrDefault(MetricAttributes.STAGE, ""));
        }
    }

    private record Group(String name, MetricType type, String unit, Producer producer) { }

    private static final class Account {
        private Instant nativeStart;
        private final Map<Map<String, String>, MetricPoint> published = new LinkedHashMap<>();
        private Map<Map<String, String>, MetricPoint> offset = Map.of();

        private Account(Instant nativeStart) { this.nativeStart = nativeStart; }
    }

    private final Map<Group, Account> accounts = new LinkedHashMap<>();

    List<MetricFact> continueNative(List<MetricFact> rawFacts, List<MetricFact> foldedFacts,
            List<MetricFact> alreadyContinued, Instant at) {
        Map<String, MetricFact> folded = byName(foldedFacts);
        Map<String, MetricFact> projected = byName(alreadyContinued);
        for (MetricFact raw : rawFacts) {
            if (raw.type() == MetricType.GAUGE || raw.points().isEmpty()
                    || CardinalityBudget.forInstrument(raw.name()).isEmpty()) {
                continue;
            }
            MetricFact publicFact = projected.get(raw.name());
            if (publicFact == null) { continue; }
            Map<Map<String, String>, MetricPoint> publicPoints = new LinkedHashMap<>();
            publicFact.points().forEach(point -> publicPoints.put(point.attributes(), point));
            Map<Producer, List<MetricPoint>> measured = groups(folded.get(raw.name()));
            for (var group : groups(raw).entrySet()) {
                Instant since = singleStart(group.getValue());
                List<MetricPoint> freshPoints = measured.getOrDefault(group.getKey(), List.of());
                // Validate before folding: an overflow merge's earliest start must not conceal mixed epochs.
                if (since == null || !since.equals(singleStart(freshPoints))) {
                    remove(publicPoints, group.getKey());
                    continue;
                }
                Group key = new Group(raw.name(), raw.type(), raw.unit(), group.getKey());
                Account account = accounts.computeIfAbsent(key, ignored -> new Account(since));
                if (since.isAfter(account.nativeStart)) {
                    account.offset = Map.copyOf(account.published);
                    account.nativeStart = since;
                }
                if (since.isBefore(account.nativeStart)) {
                    remove(publicPoints, group.getKey());
                    account.published.forEach((attributes, point) -> publicPoints.put(attributes,
                            MetricContinuation.at(point, key.type(), at)));
                    continue;
                }
                for (MetricPoint fresh : freshPoints) {
                    MetricPoint baseline = account.offset.get(fresh.attributes());
                    MetricPoint continued = baseline == null ? publicPoints.get(fresh.attributes())
                            : MetricContinuation.add(key.type(), baseline, fresh, at);
                    if (continued == null) { continue; }
                    MetricPoint previous = account.published.get(fresh.attributes());
                    if (previous != null) {
                        continued = MetricContinuation.atLeast(key.type(), continued, previous, at);
                    }
                    publicPoints.put(fresh.attributes(), continued);
                }
                // A missing operation/table retains its latest known total, including this native epoch.
                account.published.forEach((attributes, point) -> publicPoints.putIfAbsent(attributes,
                        MetricContinuation.at(point, key.type(), at)));
                publicPoints.values().stream().filter(point -> group.getKey().equals(Producer.of(point)))
                        .forEach(point -> account.published.put(point.attributes(), point));
            }
            projected.put(raw.name(), new MetricFact(publicFact.name(), publicFact.type(), publicFact.unit(),
                    List.copyOf(publicPoints.values())));
        }
        return List.copyOf(projected.values());
    }

    /** The bounded last known cumulative points, including producers absent from a quiet frame. */
    List<MetricFact> knownFacts(Instant at) {
        Map<String, MetricFact> facts = new LinkedHashMap<>();
        accounts.forEach((group, account) -> {
            var points = account.published.values().stream()
                    .map(point -> MetricContinuation.at(point, group.type(), at)).toList();
            MetricFact fact = facts.get(group.name());
            facts.put(group.name(), fact == null ? new MetricFact(group.name(), group.type(), group.unit(), points)
                    : fact.with(points));
        });
        return List.copyOf(facts.values());
    }

    private static Map<String, MetricFact> byName(List<MetricFact> facts) {
        Map<String, MetricFact> result = new LinkedHashMap<>();
        facts.forEach(fact -> result.put(fact.name(), fact));
        return result;
    }

    private static Map<Producer, List<MetricPoint>> groups(MetricFact fact) {
        Map<Producer, List<MetricPoint>> result = new LinkedHashMap<>();
        fact.points().forEach(point -> result.computeIfAbsent(Producer.of(point),
                ignored -> new ArrayList<>()).add(point));
        return result;
    }

    private static Instant singleStart(List<MetricPoint> points) {
        if (points.isEmpty()) { return null; }
        Instant start = points.getFirst().startTime();
        return start != null && points.stream().allMatch(point -> start.equals(point.startTime())) ? start : null;
    }

    private static void remove(Map<Map<String, String>, MetricPoint> points, Producer producer) {
        points.values().removeIf(point -> producer.equals(Producer.of(point)));
    }
}
