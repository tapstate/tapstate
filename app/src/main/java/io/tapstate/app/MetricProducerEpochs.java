package io.tapstate.app;

import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.spi.store.ObservationContinuation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    /** One immutable checkpoint of the real current native epochs, never an epoch history. */
    List<ObservationContinuation.ProducerState> snapshot() {
        return accounts.entrySet().stream().map(entry -> {
            Group group = entry.getKey();
            Account account = entry.getValue();
            return new ObservationContinuation.ProducerState(group.name(), group.type(), group.unit(),
                    group.producer().direction(), group.producer().stage(), account.nativeStart,
                    List.copyOf(account.offset.values()), List.copyOf(account.published.values()));
        }).toList();
    }

    /** Only an exactly qualified target restores these private epochs; public starts are not native starts. */
    static MetricProducerEpochs restore(List<ObservationContinuation.ProducerState> states,
            CardinalityBudget.Folder folder) {
        java.util.Objects.requireNonNull(states, "states");
        java.util.Objects.requireNonNull(folder, "folder");
        MetricProducerEpochs restored = new MetricProducerEpochs();
        for (ObservationContinuation.ProducerState state : states) {
            if (state.type() == MetricType.GAUGE || state.nativeStart() == null
                    || CardinalityBudget.forInstrument(state.name()).isEmpty()) {
                throw new IllegalArgumentException("a native checkpoint requires a budgeted cumulative producer");
            }
            Producer producer = new Producer(state.direction(), state.stage());
            Group group = new Group(state.name(), state.type(), state.unit(), producer);
            Account account = new Account(state.nativeStart());
            account.offset = checkpointPoints(state.name(), state.type(), state.unit(), state.offsets(),
                    producer, folder);
            account.published.putAll(checkpointPoints(state.name(), state.type(), state.unit(), state.published(),
                    producer, folder));
            if (restored.accounts.putIfAbsent(group, account) != null) {
                throw new IllegalArgumentException("a native checkpoint repeats a producer group");
            }
        }
        return restored;
    }

    /** A newer owner's durable checkpoint cannot discard a more recent known local producer. */
    static MetricProducerEpochs restore(List<ObservationContinuation.ProducerState> states,
            CardinalityBudget.Folder folder, MetricProducerEpochs previous, List<MetricFact> baseline) {
        MetricProducerEpochs restored = restore(states, folder);
        Map<String, MetricFact> bases = byName(baseline);
        previous.accounts.forEach((group, local) -> {
            Account incoming = restored.accounts.get(group);
            if (incoming == null || local.nativeStart.isAfter(incoming.nativeStart)) {
                restored.accounts.put(group, copyAccount(local));
                return;
            }
            Map<Map<String, String>, MetricPoint> base = new LinkedHashMap<>();
            MetricFact fact = bases.get(group.name());
            if (fact != null) { fact.points().forEach(point -> base.put(point.attributes(), point)); }
            Map<Map<String, String>, MetricPoint> required = incoming.nativeStart.equals(local.nativeStart)
                    ? local.offset : local.published;
            Map<Map<String, String>, MetricPoint> merged = new LinkedHashMap<>(incoming.offset);
            required.forEach((attributes, point) -> {
                MetricPoint before = incoming.offset.getOrDefault(attributes, base.get(attributes));
                MetricPoint larger = before == null ? point
                        : MetricContinuation.atLeast(group.type(), before, point, point.observedAt());
                merged.put(attributes, larger);
                MetricPoint published = incoming.published.get(attributes);
                if (published != null) {
                    incoming.published.put(attributes, increaseOffset(group.type(), published, before, larger));
                }
            });
            incoming.offset = Map.copyOf(merged);
            local.published.forEach((attributes, point) -> incoming.published.merge(attributes, point,
                    (fresh, known) -> MetricContinuation.atLeast(group.type(), fresh, known, fresh.observedAt())));
        });
        return restored;
    }

    private static Account copyAccount(Account source) {
        Account copy = new Account(source.nativeStart);
        copy.offset = Map.copyOf(source.offset); copy.published.putAll(source.published);
        return copy;
    }

    private static MetricPoint increaseOffset(MetricType type, MetricPoint published, MetricPoint before,
            MetricPoint after) {
        if (type == MetricType.COUNTER) {
            long difference = Math.subtractExact(after.value(), before == null ? 0 : before.value());
            return MetricPoint.accumulated(published.attributes(), published.startTime(), published.observedAt(),
                    Math.addExact(published.value(), difference));
        }
        HistogramValue old = before == null ? null : before.histogram();
        HistogramValue next = after.histogram();
        HistogramValue value = published.histogram();
        if (!value.bounds().equals(next.bounds()) || old != null && !old.bounds().equals(next.bounds())) {
            throw new IllegalArgumentException("a checkpoint offset keeps its histogram bounds");
        }
        List<Long> buckets = new ArrayList<>(value.bucketCounts().size());
        for (int i = 0; i < value.bucketCounts().size(); i++) {
            long difference = Math.subtractExact(next.bucketCounts().get(i), old == null ? 0 : old.bucketCounts().get(i));
            buckets.add(Math.addExact(value.bucketCounts().get(i), difference));
        }
        return MetricPoint.distribution(published.attributes(), published.startTime(), published.observedAt(),
                new HistogramValue(Math.addExact(value.count(), Math.subtractExact(next.count(), old == null ? 0 : old.count())),
                        value.sum() + next.sum() - (old == null ? 0 : old.sum()), value.bounds(), buckets));
    }

    private static Map<Map<String, String>, MetricPoint> checkpointPoints(String name, MetricType type,
            String unit, List<MetricPoint> points, Producer producer, CardinalityBudget.Folder folder) {
        MetricFact folded = folder.fold(new MetricFact(name, type, unit, points));
        Map<Map<String, String>, MetricPoint> copied = new LinkedHashMap<>();
        for (MetricPoint point : folded.points()) {
            if (!producer.equals(Producer.of(point))) {
                throw new IllegalArgumentException("a native checkpoint point belongs to another producer");
            }
            if (copied.putIfAbsent(point.attributes(), point) != null) {
                throw new IllegalArgumentException("a native checkpoint repeats a point");
            }
        }
        return Map.copyOf(copied);
    }

    List<MetricFact> continueNative(List<MetricFact> rawFacts, List<MetricFact> foldedFacts,
            List<MetricFact> alreadyContinued, Instant at) {
        Map<String, MetricFact> folded = byName(foldedFacts);
        Map<String, MetricFact> projected = byName(alreadyContinued);
        Map<String, MetricFact> offered = byName(rawFacts);
        Set<Group> supplied = new LinkedHashSet<>();
        for (MetricFact raw : rawFacts) {
            if (raw.type() != MetricType.GAUGE) {
                groups(raw).keySet().forEach(producer -> supplied.add(new Group(raw.name(), raw.type(), raw.unit(), producer)));
            }
        }
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
        accounts.forEach((group, account) -> {
            // An explicitly supplied invalid epoch is rejected above, rather than hidden by a retained point.
            if (supplied.contains(group) || account.published.isEmpty()) { return; }
            MetricFact raw = offered.get(group.name());
            MetricFact measured = folded.get(group.name());
            MetricFact current = projected.get(group.name());
            if (!sameShapeOrAbsent(group, raw) || !sameShapeOrAbsent(group, measured)
                    || !sameShapeOrAbsent(group, current)) { return; }
            Map<Map<String, String>, MetricPoint> retained = new LinkedHashMap<>();
            if (current != null) { current.points().forEach(point -> retained.put(point.attributes(), point)); }
            // A quiet native producer has no fresh measurement. Keep the exact known public point and its time.
            account.published.forEach((attributes, point) -> retained.merge(attributes, point,
                    (fresh, known) -> retainKnownHighwater(group.type(), fresh, known)));
            projected.put(group.name(), new MetricFact(group.name(), group.type(), group.unit(), List.copyOf(retained.values())));
        });
        return List.copyOf(projected.values());
    }

    private static boolean sameShapeOrAbsent(Group group, MetricFact fact) {
        return fact == null || fact.type() == group.type() && fact.unit().equals(group.unit())
                && fact.points().stream().filter(point -> group.producer().equals(Producer.of(point)))
                        .allMatch(point -> point.startTime() != null);
    }

    private static MetricPoint retainKnownHighwater(MetricType type, MetricPoint current, MetricPoint known) {
        if (type == MetricType.HISTOGRAM) {
            if (!current.histogram().bounds().equals(known.histogram().bounds())) {
                throw new IllegalArgumentException("a retained histogram cannot cross different bounds");
            }
            if (current.histogram().sum() < known.histogram().sum()) { return known; }
        }
        return MetricContinuation.atLeast(type, current, known, known.observedAt());
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
