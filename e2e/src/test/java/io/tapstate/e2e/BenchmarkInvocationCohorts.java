package io.tapstate.e2e;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.tapstate.e2e.BenchmarkJdiCostObserver.Count;
import static io.tapstate.e2e.BenchmarkJdiTelemetrySession.Segment;

/** Invocation entry cohorts with explicit pending work at every capture boundary. */
final class BenchmarkInvocationCohorts<K> {
    static final class Invocation<K> {
        private final K key;
        private Invocation(K key) { this.key = key; }
        K key() { return key; }
    }
    private static final class MutableCount {
        long entries;
        long returns;
    }
    private static final int MAX_OPEN = 16_384;
    private final Map<K, MutableCount> counts = new LinkedHashMap<>();
    private final Set<Invocation<K>> open = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
    private Map<K, Long> carryIn = Map.of();
    private Segment phase = Segment.CARRY_IN;

    Invocation<K> enter(Function<Segment, K> keyFactory) {
        if (open.size() >= MAX_OPEN) { throw invalid("open invocation budget exceeded"); }
        Invocation<K> invocation = new Invocation<>(keyFactory.apply(phase));
        open.add(invocation);
        if (phase != Segment.CARRY_IN) {
            counts.computeIfAbsent(invocation.key(), ignored -> new MutableCount()).entries++;
        }
        return invocation;
    }

    void returned(Invocation<K> invocation) {
        if (!open.remove(invocation)) { throw invalid("normal return lacked an open invocation"); }
        if (phase != Segment.CARRY_IN) {
            counts.computeIfAbsent(invocation.key(), ignored -> new MutableCount()).returns++;
        }
    }

    void begin() {
        if (phase != Segment.CARRY_IN) { throw invalid("begin was repeated"); }
        carryIn = pending(); counts.clear(); phase = Segment.WINDOW;
    }

    void cutoff() {
        if (phase != Segment.WINDOW) { throw invalid("cutoff lacked an open measured window"); }
        phase = Segment.DRAIN_TAIL;
    }

    Map<K, Long> pending() {
        Map<K, Long> pending = new HashMap<>();
        open.forEach(invocation -> pending.merge(invocation.key(), 1L, Long::sum));
        return Map.copyOf(pending);
    }

    Map<K, Long> carryIn() { return carryIn; }

    Map<K, Count> counts() {
        Map<K, Count> result = new LinkedHashMap<>();
        counts.forEach((key, count) -> result.put(key, new Count(count.entries, count.returns)));
        return Map.copyOf(result);
    }

    void requireDrained() {
        if (phase != Segment.DRAIN_TAIL || !open.isEmpty()) { throw invalid("capture has not drained"); }
        Set<K> keys = new HashSet<>(counts.keySet()); keys.addAll(carryIn.keySet());
        for (K key : keys) {
            MutableCount count = counts.get(key);
            long entries = count == null ? 0 : count.entries;
            long returns = count == null ? 0 : count.returns;
            if (Math.addExact(entries, carryIn.getOrDefault(key, 0L)) != returns) {
                throw invalid("drain did not balance pending carry-in and observed entries");
            }
        }
    }

    private static AssertionError invalid(String reason) {
        return new AssertionError("Invalid invocation cohorts: " + reason);
    }
}
