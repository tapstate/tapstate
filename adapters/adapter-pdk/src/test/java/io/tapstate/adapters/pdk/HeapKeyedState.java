package io.tapstate.adapters.pdk;

import io.tapstate.spi.store.KeyedStateStore;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A store that keeps the bytes in memory, so a case using it is about whatever sits on top of the store
 * rather than about a store. It is durable in the only sense these cases need: it outlives the connector
 * handle that wrote through it, which is exactly what the map being replaced did not.
 */
final class HeapKeyedState implements KeyedStateStore {

    private final Map<String, Map<String, byte[]>> byNamespace = new ConcurrentHashMap<>();

    @Override
    public Optional<byte[]> load(String namespace, String key) {
        return Optional.ofNullable(byNamespace.getOrDefault(namespace, Map.of()).get(key));
    }

    @Override
    public void save(String namespace, String key, byte[] state) {
        entries(namespace).put(key, state);
    }

    @Override
    public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
        return Optional.ofNullable(entries(namespace).putIfAbsent(key, state));
    }

    @Override
    public void delete(String namespace, String key) {
        entries(namespace).remove(key);
    }

    @Override
    public void dropNamespace(String namespace) {
        byNamespace.remove(namespace);
    }

    @Override
    public long count(String namespace) {
        return byNamespace.getOrDefault(namespace, Map.of()).size();
    }

    private Map<String, byte[]> entries(String namespace) {
        return byNamespace.computeIfAbsent(namespace, ignored -> new LinkedHashMap<>());
    }
}
