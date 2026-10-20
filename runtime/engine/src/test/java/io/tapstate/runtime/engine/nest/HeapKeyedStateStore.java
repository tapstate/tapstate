package io.tapstate.runtime.engine.nest;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A store that keeps what it is given, so an evicted entry has somewhere to have gone.
 *
 * <p>Public because the shape a nest map takes in production - a map with this behind it - has to be
 * reachable from the cases outside this package that assert on behaviour, not only from the ones in it.
 *
 * <p><b>Concurrent for the same reason the map in front of it is.</b> What calls this is a member's
 * partition threads, several of them at once and each for partitions of its own, so a plain map here loses
 * writes to keys no other thread was touching whenever two of them grow the table together - measured with
 * four writers on keys of their own, three quarters of the writes read back absent in one run. The store
 * this stands in for cannot lose them, so every such loss is a red no deployment can produce.
 */
public final class HeapKeyedStateStore implements io.tapstate.spi.store.NestStateStore {

    private record Row(byte[] state, byte[] pendingKey) { }
    private final Map<String, Map<String, Row>> byNamespace = new ConcurrentHashMap<>();

    @Override
    public Optional<byte[]> load(String namespace, String key) {
        Map<String, Row> entries = byNamespace.get(namespace);
        return entries == null ? Optional.empty() : Optional.ofNullable(entries.get(key)).map(Row::state);
    }

    @Override
    public void save(String namespace, String key, byte[] state) {
        saveNest(namespace, key, state, null);
    }

    @Override public void saveNest(String namespace, String key, byte[] state, byte[] pendingKey) {
        byNamespace.computeIfAbsent(namespace, ignored -> new ConcurrentHashMap<>()).put(key, new Row(state, pendingKey));
    }

    @Override public java.util.List<Pending> pending(String namespace, String after, int limit) {
        return byNamespace.getOrDefault(namespace, Map.of()).entrySet().stream()
                .filter(entry -> entry.getValue().pendingKey() != null)
                .filter(entry -> after == null || entry.getKey().compareTo(after) > 0)
                .sorted(Map.Entry.comparingByKey()).limit(limit)
                .map(entry -> new Pending(entry.getKey(), entry.getValue().pendingKey())).toList();
    }

    @Override
    public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
        return Optional.ofNullable(byNamespace
                .computeIfAbsent(namespace, ignored -> new ConcurrentHashMap<>())
                .putIfAbsent(key, new Row(state, null))).map(Row::state);
    }

    @Override
    public void delete(String namespace, String key) {
        Map<String, Row> entries = byNamespace.get(namespace);
        if (entries != null) {
            entries.remove(key);
        }
    }

    @Override
    public void dropNamespace(String namespace) {
        byNamespace.remove(namespace);
    }

    @Override
    public long count(String namespace) {
        return byNamespace.getOrDefault(namespace, Map.of()).size();
    }
}
